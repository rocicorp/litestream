package lite_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/benbjohnson/litestream/lite"
	"github.com/mattn/go-sqlite3"
)

func init() {
	sql.Register("litestream-sqlite3", &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			if err := conn.SetFileControlInt("main", sqlite3.SQLITE_FCNTL_PERSIST_WAL, 1); err != nil {
				return fmt.Errorf("cannot set file control: %w", err)
			}
			return nil
		},
	})
}

// MustOpenSQLDB returns a database/sql DB and the path to the underlying file.
func MustOpenSQLDB(tb testing.TB) (*sql.DB, string) {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "db")
	d, err := sql.Open("sqlite3", path)
	if err != nil {
		tb.Fatal(err)
	} else if _, err := d.Exec(`PRAGMA journal_mode = wal;`); err != nil {
		tb.Fatal(err)
	}
	return d, path
}

// MustCloseSQLDB closes a database/sql DB.
func MustCloseSQLDB(tb testing.TB, d *sql.DB) {
	tb.Helper()
	if err := d.Close(); err != nil {
		tb.Fatal(err)
	}
}

// TestReadPage_WideOffset verifies that ReadPage reads the correct byte offset
// for a page whose offset exceeds 4 GiB. A previous implementation computed the
// offset with `(pageNo-1) * uint32(pageSize)`, which overflows uint32 for any
// page past the 4 GiB mark and reads an unrelated page from earlier in the file.
// On large replicas this caused litestream to export a random application row's
// text (e.g. a cuid primary key) as the watermark. See lite.ReadPage.
func TestReadPage_WideOffset(t *testing.T) {
	const pageSize = 4096
	// Page whose byte offset is just past 4 GiB, so the old uint32 multiply
	// wraps to a small offset (here, 4096) instead of the true ~4 GiB offset.
	// Kept as a var (not const) so the buggy uint32 math below is evaluated at
	// runtime rather than rejected as a constant overflow.
	pageNo := uint32(1<<20) + 2 // 1048578

	correctOffset := int64(pageNo-1) * int64(pageSize)
	wrappedOffset := int64((pageNo - 1) * uint32(pageSize)) // the old, buggy math
	if wrappedOffset == correctOffset {
		t.Fatalf("test misconfigured: offset %d did not overflow uint32", correctOffset)
	}

	path := filepath.Join(t.TempDir(), "db")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Write a decoy page at the wrapped offset and the real page at the true
	// offset. The file is sparse (~4 GiB logical, a few KB physical).
	decoy := make([]byte, pageSize)
	for i := range decoy {
		decoy[i] = 0xDD
	}
	want := make([]byte, pageSize)
	for i := range want {
		want[i] = 0xCC
	}
	copy(want, []byte("correct-page"))
	if _, err := f.WriteAt(decoy, wrappedOffset); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(want, correctOffset); err != nil {
		t.Fatal(err)
	}

	got, err := lite.ReadPage(f, pageNo, pageSize)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("ReadPage read the wrong offset (got %x…, want %x…): uint32 overflow", got[:16], want[:16])
	}
}

func TestReplica_ReadTextValueFromLeafPage(t *testing.T) {
	sqldb, path := MustOpenSQLDB(t)
	defer MustCloseSQLDB(t, sqldb)

	if _, err := sqldb.Exec(`
		CREATE TABLE foo(
			a TEXT, 
			b INT, 
			b1 INT GENERATED AS (b*2) VIRTUAL, 
			b2 INT GENERATED AS (b*3) STORED, 
			b3 INT HIDDEN,  -- Note: not actually hidden; HIDDEN is only for virtual tables.
			c TEXT, 
			d FLOAT,
			d1 FLOAT GENERATED AS (d*3.14) STORED,
			d2 FLOAT GENERATED AS (d*3.1415) VIRTUAL,
			e TEXT, 
			e1 TEXT GENERATED AS (e||'foo') VIRTUAL,
			e2 TEXT GENERATED AS (e||'bar') STORED,
			f TEXT
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO foo(a, b, b3, c, d, e, f) VALUES (
	 'abc123', 293482039, 123, 'zzzxyzcba', 19382.383828937238, NULL, 'hello, world'
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO foo(a, b, b3, c, d, e, f) VALUES (
		'boomboom', 300, 321, 'bonk', 1, 130, 'zip'
	 )`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO foo(a, b, b3, c, d, e, f) VALUES (
		0, 200, 999, 16000, 120800, 82938298, 'after different int sizes'
	 )`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`PRAGMA wal_checkpoint(FULL)`); err != nil {
		t.Fatal(err)
	}
	var pageSize int
	if err := sqldb.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	var tests = []struct {
		row       int
		column    string
		watermark string
	}{
		{row: 0, column: "a", watermark: "abc123"},
		{row: 0, column: "c", watermark: "zzzxyzcba"},
		{row: 0, column: "e", watermark: ""},
		{row: 0, column: "f", watermark: "hello, world"},
		{row: 1, column: "a", watermark: "boomboom"},
		{row: 1, column: "c", watermark: "bonk"},
		{row: 1, column: "e", watermark: "130"},
		{row: 1, column: "f", watermark: "zip"},
		{row: 2, column: "a", watermark: "0"},
		{row: 2, column: "c", watermark: "16000"},
		{row: 2, column: "e", watermark: "82938298"},
		{row: 2, column: "f", watermark: "after different int sizes"},
	}
	for _, test := range tests {
		if pos, err := lite.GetDBPos(sqldb, "foo", test.column, test.row); err != nil {
			t.Fatal(err)
		} else if page, err := lite.ReadPage(file, pos.Page(), pageSize); err != nil {
			t.Fatal(err)
		} else if watermark, err := lite.ReadTextValueFromLeafPage(page, pos); err != nil {
			t.Fatal(err)
		} else if watermark != test.watermark {
			t.Fatalf("column=%s (pos=%o), expected: %s, got: %s", test.column, pos, test.watermark, watermark)
		}
	}
}
