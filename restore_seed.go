package litestream

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/superfly/ltx"
	"modernc.org/sqlite"

	"github.com/benbjohnson/litestream/internal"
)

// Seeding leaves local state from which replicate resumes incrementally
// against a forked replica instead of writing a full base snapshot:
//   - a WAL whose salt is fixed by one committed _litestream_seq transaction,
//     kept on close by SQLITE_FCNTL_PERSIST_WAL; and
//   - a level-0 anchor [h+1, h+1] for an empty transaction after the
//     restored TXID h, recording that salt at WAL offset 32, so verify takes
//     its "at WAL header, salt matches" branch. Replicate then encodes the
//     stamp transaction and everything after it as h+2 onward.
//
// A matching salt proves every change since h is still in the WAL: anything
// that discards WAL frames (restart, truncate, delete) also changes the salt,
// and a mismatch makes replicate fall back to a snapshot. Getting the local
// lifecycle wrong therefore costs speed, never data.

// The statements DB.init and DB.bumpLitestreamSeq run to force a write to an
// empty WAL, which stampSeedWAL repeats on its own connection.
const (
	createLitestreamSeqSQL = `CREATE TABLE IF NOT EXISTS _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);`
	bumpLitestreamSeqSQL   = `INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1`
)

// stampSeedWAL commits one _litestream_seq transaction to the database at
// dbPath and closes it with the WAL kept, then returns the WAL salt.
//
// A WAL holding only its header loses its salt on the next write (SQLite
// re-randomizes it for the first frame on a fresh connection), so a real
// frame is required. The last connection to close deletes the WAL unless it
// has PERSIST_WAL set; the flag is per connection, so a single connection
// does all of the work.
func stampSeedWAL(ctx context.Context, dbPath string) (salt1, salt2 uint32, err error) {
	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = sqldb.Close() }()
	sqldb.SetMaxOpenConns(1)

	conn, err := sqldb.Conn(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("get connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.Raw(func(driverConn any) error {
		fc, ok := driverConn.(sqlite.FileControl)
		if !ok {
			return fmt.Errorf("driver does not implement FileControl")
		}
		_, err := fc.FileControlPersistWAL("main", 1)
		return err
	}); err != nil {
		return 0, 0, fmt.Errorf("set PERSIST_WAL: %w", err)
	}

	var mode string
	if err := conn.QueryRowContext(ctx, `PRAGMA journal_mode = wal;`).Scan(&mode); err != nil {
		return 0, 0, fmt.Errorf("set wal mode: %w", err)
	} else if mode != "wal" {
		return 0, 0, fmt.Errorf("enable wal failed, mode=%q", mode)
	}
	if _, err := conn.ExecContext(ctx, createLitestreamSeqSQL); err != nil {
		return 0, 0, fmt.Errorf("create _litestream_seq table: %w", err)
	}
	if _, err := conn.ExecContext(ctx, bumpLitestreamSeqSQL); err != nil {
		return 0, 0, fmt.Errorf("bump _litestream_seq: %w", err)
	}

	if err := conn.Close(); err != nil {
		return 0, 0, fmt.Errorf("close connection: %w", err)
	}
	if err := sqldb.Close(); err != nil {
		return 0, 0, fmt.Errorf("close database: %w", err)
	}

	walPath := dbPath + "-wal"
	if fi, err := os.Stat(walPath); err != nil {
		return 0, 0, fmt.Errorf("stat wal: %w", err)
	} else if fi.Size() <= WALHeaderSize {
		return 0, 0, fmt.Errorf("wal not kept on close: size %d", fi.Size())
	}
	hdr, err := readWALHeader(walPath)
	if err != nil {
		return 0, 0, fmt.Errorf("read wal header: %w", err)
	}
	return binary.BigEndian.Uint32(hdr[16:]), binary.BigEndian.Uint32(hdr[20:]), nil
}

// seedLocalState stamps the WAL of the restored database at dbPath and
// writes the local anchor for the empty transaction txID (the restored TXID
// plus one) into l0Dir. It returns the anchor's path.
//
// The anchor holds page 1 as it was before stamping, with the page count of
// that time, so applying it changes nothing: the database at txID is the
// database at txID-1. It is also uploaded as the fork replica's level-0 head
// (see restoreForker.finish), where it is part of the replica's history.
func seedLocalState(ctx context.Context, dbPath, l0Dir string, dirInfo os.FileInfo, txID ltx.TXID) (string, error) {
	// The restore leaves the database file at exactly its commit size, and
	// page 1 is read before stamping, so both describe the restored database.
	pageSize, page1, err := readPage1(dbPath)
	if err != nil {
		return "", err
	}
	fi, err := os.Stat(dbPath)
	if err != nil {
		return "", err
	} else if fi.Size()%int64(pageSize) != 0 {
		return "", fmt.Errorf("database size %d is not a multiple of page size %d", fi.Size(), pageSize)
	}
	commit := uint32(fi.Size() / int64(pageSize))

	salt1, salt2, err := stampSeedWAL(ctx, dbPath)
	if err != nil {
		return "", fmt.Errorf("stamp wal: %w", err)
	}
	return writeLocalAnchor(l0Dir, dirInfo, ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  pageSize,
		Commit:    commit,
		MinTXID:   txID,
		MaxTXID:   txID,
		Timestamp: time.Now().UnixMilli(),
		WALOffset: WALHeaderSize,
		WALSize:   0,
		WALSalt1:  salt1,
		WALSalt2:  salt2,
	}, page1)
}

// writeLocalAnchor replaces the contents of the local level-0 directory with
// a one-page LTX file with header hdr, and returns its path.
func writeLocalAnchor(l0Dir string, dirInfo os.FileInfo, hdr ltx.Header, page1 []byte) (string, error) {
	if err := os.RemoveAll(l0Dir); err != nil {
		return "", fmt.Errorf("clear level-0 directory: %w", err)
	}
	if err := internal.MkdirAll(l0Dir, dirInfo); err != nil {
		return "", fmt.Errorf("create level-0 directory: %w", err)
	}

	path := filepath.Join(l0Dir, ltx.FormatFilename(hdr.MinTXID, hdr.MaxTXID))
	tmpPath := path + ".tmp"
	defer func() { _ = os.Remove(tmpPath) }()

	f, err := os.Create(tmpPath)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	enc, err := ltx.NewEncoder(f)
	if err != nil {
		return "", fmt.Errorf("new ltx encoder: %w", err)
	} else if err := enc.EncodeHeader(hdr); err != nil {
		return "", fmt.Errorf("encode anchor header: %w", err)
	} else if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, page1); err != nil {
		return "", fmt.Errorf("encode anchor page: %w", err)
	} else if err := enc.Close(); err != nil {
		return "", fmt.Errorf("close anchor encoder: %w", err)
	}

	if err := f.Sync(); err != nil {
		return "", err
	} else if err := f.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return "", err
	}
	if err := internal.FsyncDir(l0Dir); err != nil {
		return "", fmt.Errorf("sync level-0 directory: %w", err)
	}
	return path, nil
}

// readPage1 returns the page size and page 1 of the SQLite database at
// dbPath, read directly from the file.
func readPage1(dbPath string) (uint32, []byte, error) {
	f, err := os.Open(dbPath)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = f.Close() }()

	hdr := make([]byte, 100)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return 0, nil, fmt.Errorf("read database header: %w", err)
	}
	pageSize := uint32(binary.BigEndian.Uint16(hdr[16:18]))
	if pageSize == 1 {
		pageSize = 65536
	}
	if pageSize < 512 || pageSize&(pageSize-1) != 0 {
		return 0, nil, fmt.Errorf("invalid database page size: %d", pageSize)
	}

	page := make([]byte, pageSize)
	if _, err := f.ReadAt(page, 0); err != nil {
		return 0, nil, fmt.Errorf("read page 1: %w", err)
	}
	return pageSize, page, nil
}
