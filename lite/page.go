package lite

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// PagePos is the position of a value within a page.
type PagePos interface {
	Row() int
	Col() int
}

// DBPos is the position of a value within the database.
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

// GetDBPos returns the position of the value specified by table, column, and
// row in ROWID order. The table must fit within a single database page so the
// table rootpage points directly to the table b-tree leaf page.
func GetDBPos(db *sql.DB, table string, column string, row int) (*DBPos, error) {
	var pageNo uint32
	var cid int
	if err := db.QueryRow(`SELECT cid FROM pragma_table_xinfo(?) WHERE name = ?`, table, column).
		Scan(&cid); err != nil {
		return nil, fmt.Errorf(`lookup db pos ("%s"."%s") cid: %w`, table, column, err)
	}

	var virtualN int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_xinfo(?) WHERE cid < ? AND hidden = 2`, table, cid).
		Scan(&virtualN); err != nil {
		return nil, fmt.Errorf(`lookup db pos ("%s"."%s") virtual columns: %w`, table, column, err)
	}

	if err := db.QueryRow(`SELECT rootpage FROM sqlite_schema WHERE name = ?`, table).
		Scan(&pageNo); err != nil {
		return nil, fmt.Errorf("lookup %s rootpage: %w", table, err)
	}

	// GENERATED ... VIRTUAL columns have schema column IDs but are not stored in
	// the page record. Subtract preceding virtual columns to get the stored
	// record column position.
	return NewDBPos(pageNo, row, cid-virtualN), nil
}

// ReadPage reads pageNo from the main database file.
func ReadPage(db *os.File, pageNo uint32, pageSize int) ([]byte, error) {
	if pageNo == 0 {
		return nil, fmt.Errorf("invalid page number: %d", pageNo)
	}
	if pageSize <= 0 {
		return nil, fmt.Errorf("invalid page size: %d", pageSize)
	}

	page := make([]byte, pageSize)
	if _, err := db.ReadAt(page, int64(pageNo-1)*int64(pageSize)); err != nil {
		return nil, err
	}
	return page, nil
}

// ReadTextValueFromLeafPage reads a text value from the page.
func ReadTextValueFromLeafPage(page []byte, pagePos PagePos) (string, error) {
	if len(page) < 8 {
		return "", fmt.Errorf("page too small: %d bytes", len(page))
	}
	if pagePos.Row() < 0 {
		return "", fmt.Errorf("invalid row: %d", pagePos.Row())
	}
	if pagePos.Col() < 0 {
		return "", fmt.Errorf("invalid column: %d", pagePos.Col())
	}

	headerOffset := 0
	if page[0] != 0x0d && len(page) > 100 && page[100] == 0x0d {
		headerOffset = 100
	}
	if page[headerOffset] != 0x0d {
		return "", fmt.Errorf("unexpected page type %x", page[headerOffset])
	}

	row := pagePos.Row()
	rowN := int(binary.BigEndian.Uint16(page[headerOffset+3 : headerOffset+5]))
	if rowN <= row {
		return "", fmt.Errorf("insufficient rows %d for row %d", rowN, row)
	}

	cellPtrOffset := headerOffset + 8 + (row * 2)
	if cellPtrOffset+2 > len(page) {
		return "", fmt.Errorf("cell pointer out of bounds: %d", cellPtrOffset)
	}
	cellOffset := int(binary.BigEndian.Uint16(page[cellPtrOffset : cellPtrOffset+2]))
	if cellOffset >= len(page) {
		return "", fmt.Errorf("cell offset out of bounds: %d", cellOffset)
	}

	payload := page[cellOffset:]
	_, n, err := readSQLiteVarint(payload) // payload size
	if err != nil {
		return "", fmt.Errorf("read payload size: %w", err)
	}
	pos := n
	if _, n, err = readSQLiteVarint(payload[pos:]); err != nil { // rowid
		return "", fmt.Errorf("read rowid: %w", err)
	}
	pos += n

	recordStart := pos
	recordHeaderLen, n, err := readSQLiteVarint(payload[recordStart:])
	if err != nil {
		return "", fmt.Errorf("read record header length: %w", err)
	}

	headerPos := recordStart + n
	headerEnd := recordStart + int(recordHeaderLen)
	if headerEnd > len(payload) {
		return "", fmt.Errorf("record header out of bounds: %d", headerEnd)
	}

	valuePos := headerEnd
	for col := 0; headerPos < headerEnd; col++ {
		serialType, n, err := readSQLiteVarint(payload[headerPos:headerEnd])
		if err != nil {
			return "", fmt.Errorf("read serial type for column %d: %w", col, err)
		}
		headerPos += n

		valueLen := lengthOf(serialType)
		if valuePos+valueLen > len(payload) {
			return "", fmt.Errorf("value out of bounds for column %d", col)
		}
		if col == pagePos.Col() {
			return string(payload[valuePos : valuePos+valueLen]), nil
		}
		valuePos += valueLen
	}

	return "", nil
}

func readSQLiteVarint(p []byte) (uint64, int, error) {
	var v uint64
	for i := 0; i < 9; i++ {
		if i >= len(p) {
			return 0, 0, io.ErrUnexpectedEOF
		}

		b := p[i]
		if i == 8 {
			return (v << 8) | uint64(b), 9, nil
		}

		v = (v << 7) | uint64(b&0x7f)
		if b < 0x80 {
			return v, i + 1, nil
		}
	}
	panic("unreachable")
}

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
	if serial%2 == 0 {
		return (serial - 12) / 2 // BLOB
	}
	return (serial - 13) / 2 // TEXT
}
