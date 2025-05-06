package lite

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	"os"
)

// The position of a value within a page.
type PagePos interface {
	Row() int
	Col() int
}

// The position of a value within the database.
type DBPos struct {
	page uint32
	row  int
	col  int
}

func NewDBPos(page uint32, row int, col int) *DBPos {
	return &DBPos{page: page, row: row, col: col}
}

func (pos *DBPos) Page() uint32 { return pos.page }
func (pos *DBPos) Row() int     { return pos.row }
func (pos *DBPos) Col() int     { return pos.col }

// Gets the position of the value specified by the given `table`, `column`
// and `row` (in ROWID order). This assumes that the table fits within a
// single database page, i.e. that the `rootpage` of the table in the
// `sqlite_schema` table points directly to the b-tree leaf page.
func GetDBPos(db *sql.DB, table string, column string, row int) (*DBPos, error) {
	var pageNo uint32
	var cid int
	var numVirtual int
	if err := db.QueryRow(`SELECT cid FROM pragma_table_xinfo(?) where name = ?`, table, column).
		Scan(&cid); err != nil {
		return nil, fmt.Errorf(`lookup db pos ("%s"."%s") cid: %w`, table, column, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_xinfo(?) where cid < ? and hidden = 2`, table, cid).
		Scan(&numVirtual); err != nil {
		return nil, fmt.Errorf(`lookup db pos ("%s"."%s") numVirtual: %w`, table, column, err)
	}
	if err := db.QueryRow(`SELECT rootpage FROM sqlite_schema WHERE name = ?`, table).
		Scan(&pageNo); err != nil {
		return nil, fmt.Errorf("lookup %s rootpage: %w", table, err)
	}
	// Subtract the number of `GENERATED ... VIRTUAL` columns from the cid to
	// get the position of the value in the page.
	return NewDBPos(pageNo, row, cid-numVirtual), nil
}

// Reads the page for the given `pageNo` from the main db file.
func ReadPage(db *os.File, pageNo uint32, pageSize int) ([]byte, error) {
	page := make([]byte, pageSize)
	if _, err := db.ReadAt(page, int64((pageNo-1)*uint32(pageSize))); err != nil {
		return nil, err
	}
	return page, nil
}

// Reads the value with the given `pagePos` from the `page`.
func ReadTextValueFromLeafPage(page []byte, pagePos PagePos) (string, error) {
	// B-tree page header format: https://sqlite.org/fileformat.html#cell_payload
	if page[0] != 0xD { // Table b-tree leaf page
		return "", fmt.Errorf("unexpected page type %x", page[0])
	}
	row := pagePos.Row()
	numRows := int(binary.BigEndian.Uint16(page[3:5]))
	if numRows <= row {
		return "", fmt.Errorf("insufficient number of rows %d for row %d", numRows, row)
	}
	// Lookup the offset to the specified row. These offsets start after the
	// 8-byte page header, as sequence of 2-byte offsets per row.
	pageOffset := binary.BigEndian.Uint16(page[8+(row*2):])

	// https://sqlite.org/fileformat.html#cellformat
	payload := page[pageOffset:]
	pos := 0
	// Advanced past the first two varints of the payload: payloadSize, rowID
	for i := 0; i < 2; i++ {
		_, bytes := binary.Uvarint(payload[pos:])
		pos += bytes
	}
	// Record format: https://sqlite.org/fileformat.html#record_format
	header := bytes.NewReader(payload[pos:])
	// The header consists of an initial (varint) header size, followed
	// by varint sizes of each stored column value.
	recordHeaderLen, err := binary.ReadUvarint(header)
	if err != nil {
		return "", err
	}
	// pos points to column values (directly after the header)
	pos += int(recordHeaderLen)
	for i := 0; ; i++ {
		serialType, err := binary.ReadUvarint(header)
		if err != nil {
			return "", nil
		}
		valueLen := lengthOf(serialType)
		if i == pagePos.Col() {
			return string(payload[pos : pos+valueLen]), nil
		}
		pos += valueLen
	}
}

// https://sqlite.org/fileformat.html#serialtype
func lengthOf(serialType uint64) int {
	serial := int(serialType)
	switch serial {
	case 1, 2, 3, 4:
		return serial
	case 5:
		return 6
	case 6, 7:
		return 8
	case 0, 8, 9, 10, 11, 12, 13:
		return 0
	}
	// BLOB
	if serial%2 == 0 {
		return (serial - 12) / 2
	}
	// TEXT
	return (serial - 13) / 2
}
