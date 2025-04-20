package lite_test

import (
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

func TestReplica_ReadTextValueFromLeafPage(t *testing.T) {
	sqldb, path := MustOpenSQLDB(t)
	defer MustCloseSQLDB(t, sqldb)

	if _, err := sqldb.Exec(`CREATE TABLE foo(a TEXT, b INT, c TEXT, d FLOAT, e TEXT, f TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO foo(a, b, c, d, e, f) VALUES (
	 'abc123', 293482039, 'zzzxyzcba', 19382.383828937238, NULL, 'hello, world'
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO foo(a, b, c, d, e, f) VALUES (
		'boomboom', 300, 'bonk', 1, 130, 'zip'
	 )`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO foo(a, b, c, d, e, f) VALUES (
		0, 200, 16000, 120800, 82938298, 'after different int sizes'
	 )`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`PRAGMA wal_checkpoint(FULL)`); err != nil {
		t.Fatal(err)
	}
	var pageNo uint32
	var pageSize int
	if err := sqldb.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
		t.Fatal(err)
	}
	if err := sqldb.QueryRow(`SELECT rootpage FROM sqlite_schema WHERE name = 'foo'`).Scan(&pageNo); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	page, err := lite.ReadPage(file, pageNo, pageSize)
	if err != nil {
		t.Fatal(err)
	}

	var tests = []struct {
		row       int
		cid       int
		watermark string
	}{
		{row: 0, cid: 0, watermark: "abc123"},
		{row: 0, cid: 2, watermark: "zzzxyzcba"},
		{row: 0, cid: 4, watermark: ""},
		{row: 0, cid: 5, watermark: "hello, world"},
		{row: 1, cid: 0, watermark: "boomboom"},
		{row: 1, cid: 2, watermark: "bonk"},
		{row: 1, cid: 4, watermark: "130"},
		{row: 1, cid: 5, watermark: "zip"},
		{row: 2, cid: 0, watermark: "0"},
		{row: 2, cid: 2, watermark: "16000"},
		{row: 2, cid: 4, watermark: "82938298"},
		{row: 2, cid: 5, watermark: "after different int sizes"},
	}
	for _, test := range tests {
		pos := lite.NewDBPos(pageNo, test.row, test.cid)
		if watermark, err := lite.ReadTextValueFromLeafPage(page, pos); err != nil {
			t.Fatal(err)
		} else if watermark != test.watermark {
			t.Fatalf("cid=%d, expected: %s, got: %s", test.cid, test.watermark, watermark)
		}
	}
}
