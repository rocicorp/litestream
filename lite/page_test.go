package lite_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/benbjohnson/litestream/lite"
	_ "modernc.org/sqlite"
)

func TestReadTextValueFromLeafPage(t *testing.T) {
	sqldb, path := mustOpenSQLDB(t)
	defer sqldb.Close()

	if _, err := sqldb.Exec(`
		CREATE TABLE foo(
			a TEXT,
			b INT,
			b1 INT GENERATED ALWAYS AS (b*2) VIRTUAL,
			b2 INT GENERATED ALWAYS AS (b*3) STORED,
			b3 INT HIDDEN,
			c TEXT,
			d FLOAT,
			d1 FLOAT GENERATED ALWAYS AS (d*3.14) STORED,
			d2 FLOAT GENERATED ALWAYS AS (d*3.1415) VIRTUAL,
			e TEXT,
			e1 TEXT GENERATED ALWAYS AS (e||'foo') VIRTUAL,
			e2 TEXT GENERATED ALWAYS AS (e||'bar') STORED,
			f TEXT
		)
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO foo(a, b, b3, c, d, e, f) VALUES
		('abc123', 293482039, 123, 'zzzxyzcba', 19382.383828937238, NULL, 'hello, world'),
		('boomboom', 300, 321, 'bonk', 1, 130, 'zip'),
		(0, 200, 999, 16000, 120800, 82938298, 'after different int sizes')
	`); err != nil {
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
	defer file.Close()

	tests := []struct {
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

	for _, tt := range tests {
		pos, err := lite.GetDBPos(sqldb, "foo", tt.column, tt.row)
		if err != nil {
			t.Fatal(err)
		}
		page, err := lite.ReadPage(file, pos.Page(), pageSize)
		if err != nil {
			t.Fatal(err)
		}
		watermark, err := lite.ReadTextValueFromLeafPage(page, pos)
		if err != nil {
			t.Fatal(err)
		}
		if watermark != tt.watermark {
			t.Fatalf("column=%s row=%d, got %q, want %q", tt.column, tt.row, watermark, tt.watermark)
		}
	}
}

func mustOpenSQLDB(tb testing.TB) (*sql.DB, string) {
	tb.Helper()

	path := filepath.Join(tb.TempDir(), "db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA journal_mode = wal`); err != nil {
		tb.Fatal(err)
	}
	return db, path
}
