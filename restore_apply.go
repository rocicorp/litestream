package litestream

import (
	"fmt"
	"io"
	"os"

	"github.com/superfly/ltx"
)

// applyRestoreDeltas applies the files of a restore plan that follow the
// snapshot, in plan order, to a database file that already holds the decoded
// snapshot.
//
// Restore used to merge the snapshot and every delta through an ltx.Compactor
// and decode the merged stream. For the common plan shape, one large snapshot
// followed by a tail of small deltas, that decompressed, recompressed,
// re-hashed and decompressed again every snapshot page only to overwrite a
// small fraction of them. Decoding the snapshot directly and writing each
// delta's pages in place produces the same bytes and keeps every check the
// merge made:
//
//   - each file's CRC, and the snapshot's post-apply checksum when it carries
//     one, are verified when that file's decoder is closed;
//   - page sizes must match and transaction IDs must be contiguous;
//   - every page of the final database must have been written by the snapshot
//     or a delta, which the merge enforced by decoding a dense page stream.
//
// After each file the database is truncated to that file's commit size, which
// drops pages an earlier file wrote and the database later freed, as the merge
// did by skipping pages beyond the final commit. Page 1 is written verbatim:
// the journal-mode rewrite in applyLTXFile is for follow mode's live readers.
func applyRestoreDeltas(f *os.File, snapshot ltx.Header, rdrs []io.Reader) error {
	pageSize := snapshot.PageSize
	written := newPageSet(snapshot.Commit)
	commit, prevMaxTXID := snapshot.Commit, snapshot.MaxTXID
	data := make([]byte, pageSize)

	for _, rd := range rdrs {
		dec := ltx.NewDecoder(rd)
		if err := dec.DecodeHeader(); err != nil {
			return fmt.Errorf("decode header of ltx file after %s: %w", prevMaxTXID, err)
		}
		hdr := dec.Header()
		name := ltx.FormatFilename(hdr.MinTXID, hdr.MaxTXID)
		if hdr.PageSize != pageSize {
			return fmt.Errorf("ltx file %s: page size %d does not match snapshot page size %d", name, hdr.PageSize, pageSize)
		} else if hdr.MinTXID != prevMaxTXID+1 {
			return fmt.Errorf("non-contiguous transaction ids in restore plan: %s follows %s", name, prevMaxTXID)
		}

		for {
			var phdr ltx.PageHeader
			if err := dec.DecodePage(&phdr, data); err == io.EOF {
				break
			} else if err != nil {
				return fmt.Errorf("ltx file %s: decode page: %w", name, err)
			}
			if _, err := f.WriteAt(data, int64(phdr.Pgno-1)*int64(pageSize)); err != nil {
				return fmt.Errorf("ltx file %s: write page %d: %w", name, phdr.Pgno, err)
			}
			written.add(phdr.Pgno)
		}
		if err := dec.Close(); err != nil {
			return fmt.Errorf("ltx file %s: %w", name, err)
		}

		if hdr.Commit > 0 {
			commit = hdr.Commit
			if err := f.Truncate(int64(commit) * int64(pageSize)); err != nil {
				return fmt.Errorf("ltx file %s: truncate to %d pages: %w", name, commit, err)
			}
			written.truncate(commit)
		}
		prevMaxTXID = hdr.MaxTXID
	}

	if pgno := written.firstMissing(commit, ltx.LockPgno(pageSize)); pgno != 0 {
		return fmt.Errorf("restored database is missing page %d of %d", pgno, commit)
	}
	return nil
}

// pageSet is a bitmap of page numbers. Bit n records page n; bit 0 is unused.
// A 64 GiB database of 4 KiB pages needs 2 MiB.
type pageSet struct {
	words []uint64
}

// newPageSet returns a set holding pages 1 through n.
func newPageSet(n uint32) *pageSet {
	s := &pageSet{words: make([]uint64, n/64+1)}
	for pgno := uint32(1); pgno <= n; pgno++ {
		s.add(pgno)
	}
	return s
}

func (s *pageSet) add(pgno uint32) {
	i := int(pgno / 64)
	if i >= len(s.words) {
		s.words = append(s.words, make([]uint64, i+1-len(s.words))...)
	}
	s.words[i] |= 1 << (pgno % 64)
}

func (s *pageSet) has(pgno uint32) bool {
	i := int(pgno / 64)
	return i < len(s.words) && s.words[i]&(1<<(pgno%64)) != 0
}

// truncate removes every page above n.
func (s *pageSet) truncate(n uint32) {
	i := int(n / 64)
	if i >= len(s.words) {
		return
	}
	s.words[i] &= (uint64(1) << (n%64 + 1)) - 1 // keep bits 0 through n%64
	clear(s.words[i+1:])
}

// firstMissing returns the lowest page in 1 through n, other than skip, that
// is not in the set, or 0 if there is none.
func (s *pageSet) firstMissing(n, skip uint32) uint32 {
	for pgno := uint32(1); pgno <= n; pgno++ {
		if pgno != skip && !s.has(pgno) {
			return pgno
		}
	}
	return 0
}
