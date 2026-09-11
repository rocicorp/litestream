package litestream_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc64"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
)

const restoreTestPageSize = 512

// restoreTestFile is one LTX file in a synthetic replica.
type restoreTestFile struct {
	level            int
	minTXID, maxTXID ltx.TXID
	commit           uint32
	pages            map[uint32][]byte
}

// encode returns the file as LTX bytes. Files starting at TXID 1 are
// snapshots and must list every page from 1 through commit.
func (tf restoreTestFile) encode(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	// A zero-length database must carry the empty-database checksum, which a
	// file with checksums disabled cannot, so that one case is checksummed.
	flags, preApply := ltx.HeaderFlagNoChecksum, ltx.Checksum(0)
	if tf.commit == 0 {
		flags, preApply = 0, ltx.ChecksumFlag|1
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:          ltx.Version,
		Flags:            flags,
		PageSize:         restoreTestPageSize,
		Commit:           tf.commit,
		MinTXID:          tf.minTXID,
		MaxTXID:          tf.maxTXID,
		Timestamp:        time.Now().UnixMilli(),
		PreApplyChecksum: preApply,
	}); err != nil {
		t.Fatal(err)
	}
	pgnos := make([]uint32, 0, len(tf.pages))
	for pgno := range tf.pages {
		pgnos = append(pgnos, pgno)
	}
	slices.Sort(pgnos)
	for _, pgno := range pgnos {
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, tf.pages[pgno]); err != nil {
			t.Fatal(err)
		}
	}
	if tf.commit == 0 {
		enc.SetPostApplyChecksum(ltx.ChecksumFlag)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// patchPageNumber rewrites the page number of the nth page header in an
// encoded LTX file and recomputes the file checksum, producing a
// checksum-valid file that the encoder would have refused to write.
func patchPageNumber(b []byte, n int, pgno uint32) []byte {
	off := ltx.HeaderSize
	for i := 0; i < n; i++ {
		size := binary.BigEndian.Uint32(b[off+ltx.PageHeaderSize:])
		off += ltx.PageHeaderSize + 4 + int(size)
	}
	binary.BigEndian.PutUint32(b[off:], pgno)
	sum := crc64.Checksum(b[:len(b)-8], crc64.MakeTable(crc64.ISO))
	binary.BigEndian.PutUint64(b[len(b)-8:], uint64(ltx.ChecksumFlag)|sum)
	return b
}

// restoreTestPages returns random page contents for the given page numbers.
func restoreTestPages(rng *rand.Rand, pgnos ...uint32) map[uint32][]byte {
	m := make(map[uint32][]byte, len(pgnos))
	for _, pgno := range pgnos {
		data := make([]byte, restoreTestPageSize)
		rng.Read(data)
		m[pgno] = data
	}
	return m
}

func pageRange(lo, hi uint32) []uint32 {
	var s []uint32
	for pgno := lo; pgno <= hi; pgno++ {
		s = append(s, pgno)
	}
	return s
}

// writeRestoreTestReplica writes files into a file replica and returns a
// replica over it, plus the encoded bytes of each file in order.
func writeRestoreTestReplica(t *testing.T, files []restoreTestFile, mutate func(i int, b []byte) []byte) (*litestream.Replica, [][]byte) {
	t.Helper()
	client := file.NewReplicaClient(t.TempDir())
	encoded := make([][]byte, len(files))
	for i, tf := range files {
		b := tf.encode(t)
		if mutate != nil {
			b = mutate(i, b)
		}
		encoded[i] = b
		if _, err := client.WriteLTXFile(context.Background(), tf.level, tf.minTXID, tf.maxTXID, bytes.NewReader(b)); err != nil {
			t.Fatal(err)
		}
	}
	return litestream.NewReplicaWithClient(nil, client), encoded
}

// compactedDatabase is the reference restore: merge every file through an
// ltx.Compactor and decode the result, as Restore did before it applied
// deltas in place.
func compactedDatabase(t *testing.T, encoded [][]byte) []byte {
	t.Helper()
	rdrs := make([]io.Reader, len(encoded))
	for i, b := range encoded {
		rdrs[i] = bytes.NewReader(b)
	}
	var merged bytes.Buffer
	c, err := ltx.NewCompactor(&merged, rdrs)
	if err != nil {
		t.Fatal(err)
	}
	c.HeaderFlags = ltx.HeaderFlagNoChecksum
	if err := c.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	var db bytes.Buffer
	if err := ltx.NewDecoder(&merged).DecodeDatabaseTo(&db); err != nil {
		t.Fatal(err)
	}
	return db.Bytes()
}

// TestReplica_Restore_AppliesDeltasInPlace verifies that restoring a snapshot
// plus deltas produces exactly the bytes the compactor merge produced, across
// a delta that grows the database, one that shrinks it, one that regrows it,
// and pages overwritten by several deltas.
func TestReplica_Restore_AppliesDeltasInPlace(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	files := []restoreTestFile{
		{level: litestream.SnapshotLevel, minTXID: 1, maxTXID: 1, commit: 100, pages: restoreTestPages(rng, pageRange(1, 100)...)},
		// Grows to 120 pages and rewrites pages 3 and 50.
		{level: 3, minTXID: 2, maxTXID: 3, commit: 120, pages: restoreTestPages(rng, append([]uint32{3, 50}, pageRange(101, 120)...)...)},
		// Shrinks to 80 pages, dropping 81 through 120, and rewrites page 10.
		{level: 2, minTXID: 4, maxTXID: 4, commit: 80, pages: restoreTestPages(rng, 10)},
		// Regrows to 90 pages with fresh 81 through 90 and rewrites page 3 again.
		{level: 1, minTXID: 5, maxTXID: 6, commit: 90, pages: restoreTestPages(rng, append([]uint32{1, 3}, pageRange(81, 90)...)...)},
	}
	r, encoded := writeRestoreTestReplica(t, files, nil)

	outputPath := filepath.Join(t.TempDir(), "restored.db")
	if err := r.Restore(context.Background(), litestream.RestoreOptions{OutputPath: outputPath}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}

	if want := compactedDatabase(t, encoded); !bytes.Equal(got, want) {
		t.Fatalf("restored database differs from the compactor merge: got %d bytes, want %d", len(got), len(want))
	}

	// Independently of the compactor: last writer wins, then the final size.
	want := make([]byte, 0, 90*restoreTestPageSize)
	for pgno := uint32(1); pgno <= 90; pgno++ {
		var page []byte
		for _, tf := range files {
			if p, ok := tf.pages[pgno]; ok {
				page = p
			}
		}
		want = append(want, page...)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("restored database differs from applying each file's pages in order")
	}
}

// TestReplica_Restore_OverlappingPlan verifies the plan shape CalcRestorePlan
// produces when a compacted file covers an earlier file's whole range: a
// snapshot 1-5 followed by 1-100, with the 50-60 file inside it skipped. The
// merge accepted this through ltx.IsContiguous, so in-place apply must too.
func TestReplica_Restore_OverlappingPlan(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	files := []restoreTestFile{
		{level: litestream.SnapshotLevel, minTXID: 1, maxTXID: 5, commit: 100, pages: restoreTestPages(rng, pageRange(1, 100)...)},
		{level: 2, minTXID: 1, maxTXID: 100, commit: 110, pages: restoreTestPages(rng, pageRange(1, 110)...)},
		{level: 2, minTXID: 50, maxTXID: 60, commit: 110, pages: restoreTestPages(rng, 7)},
	}
	r, encoded := writeRestoreTestReplica(t, files, nil)

	outputPath := filepath.Join(t.TempDir(), "restored.db")
	if err := r.Restore(context.Background(), litestream.RestoreOptions{OutputPath: outputPath}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if want := compactedDatabase(t, encoded[:2]); !bytes.Equal(got, want) {
		t.Fatalf("restored database differs from the compactor merge of the plan: got %d bytes, want %d", len(got), len(want))
	}
}

// TestReplica_Restore_ZeroCommit verifies that a delta with a commit size of
// zero, an empty database in the LTX format, empties the restored file, and
// that later deltas regrow it from nothing.
func TestReplica_Restore_ZeroCommit(t *testing.T) {
	for _, tt := range []struct {
		name    string
		regrow  bool
		wantLen int
	}{
		{name: "EndsEmpty", wantLen: 0},
		{name: "RegrowsAfterEmpty", regrow: true, wantLen: 5 * restoreTestPageSize},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(4))
			files := []restoreTestFile{
				{level: litestream.SnapshotLevel, minTXID: 1, maxTXID: 1, commit: 100, pages: restoreTestPages(rng, pageRange(1, 100)...)},
				{level: 1, minTXID: 2, maxTXID: 2, commit: 0},
			}
			if tt.regrow {
				files = append(files, restoreTestFile{level: 1, minTXID: 3, maxTXID: 3, commit: 5, pages: restoreTestPages(rng, pageRange(1, 5)...)})
			}
			r, _ := writeRestoreTestReplica(t, files, nil)

			outputPath := filepath.Join(t.TempDir(), "restored.db")
			if err := r.Restore(context.Background(), litestream.RestoreOptions{OutputPath: outputPath}); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(outputPath)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != tt.wantLen {
				t.Fatalf("restored database is %d bytes, want %d", len(got), tt.wantLen)
			}
			if tt.regrow {
				var want []byte
				for pgno := uint32(1); pgno <= 5; pgno++ {
					want = append(want, files[2].pages[pgno]...)
				}
				if !bytes.Equal(got, want) {
					t.Fatal("regrown database does not match the pages written after the empty commit")
				}
			}
		})
	}
}

// TestReplica_Restore_DeltaErrors verifies that a delta which cannot be
// applied fails the restore and leaves no output behind.
func TestReplica_Restore_DeltaErrors(t *testing.T) {
	snapshot := func(rng *rand.Rand) restoreTestFile {
		return restoreTestFile{level: litestream.SnapshotLevel, minTXID: 1, maxTXID: 1, commit: 100, pages: restoreTestPages(rng, pageRange(1, 100)...)}
	}

	for _, tt := range []struct {
		name   string
		delta  func(rng *rand.Rand) restoreTestFile
		mutate func(i int, b []byte) []byte
		want   string
	}{
		{
			// Grows to 105 pages but never writes page 105; the compactor
			// merge rejected this as a gap in its dense page stream.
			name: "MissingPage",
			delta: func(rng *rand.Rand) restoreTestFile {
				return restoreTestFile{level: 1, minTXID: 2, maxTXID: 2, commit: 105, pages: restoreTestPages(rng, pageRange(101, 104)...)}
			},
			want: "missing page 105 of 105",
		},
		{
			name: "CorruptChecksum",
			delta: func(rng *rand.Rand) restoreTestFile {
				return restoreTestFile{level: 1, minTXID: 2, maxTXID: 2, commit: 100, pages: restoreTestPages(rng, 7)}
			},
			mutate: func(i int, b []byte) []byte {
				if i == 1 {
					b[len(b)-1] ^= 0xff // last byte of the file checksum
				}
				return b
			},
			want: "checksum mismatch",
		},
		{
			// A checksum-valid delta whose first page header names SQLite's
			// lock page. The encoder refuses to write one, so the page number
			// is patched afterwards and the file checksum recomputed.
			name: "LockPage",
			delta: func(rng *rand.Rand) restoreTestFile {
				lock := ltx.LockPgno(restoreTestPageSize)
				return restoreTestFile{level: 1, minTXID: 2, maxTXID: 2, commit: lock, pages: restoreTestPages(rng, lock-1)}
			},
			mutate: func(i int, b []byte) []byte {
				if i == 1 {
					return patchPageNumber(b, 0, ltx.LockPgno(restoreTestPageSize))
				}
				return b
			},
			want: "contains lock page",
		},
		{
			// Pages 5 then 3: the merge rejected this when re-encoding.
			name: "PagesOutOfOrder",
			delta: func(rng *rand.Rand) restoreTestFile {
				return restoreTestFile{level: 1, minTXID: 2, maxTXID: 2, commit: 100, pages: restoreTestPages(rng, 5, 9)}
			},
			mutate: func(i int, b []byte) []byte {
				if i == 1 {
					return patchPageNumber(b, 1, 3)
				}
				return b
			},
			want: "page 3 out of order after page 5",
		},
		{
			// A page past the file's own commit size, which could otherwise
			// be written at an arbitrary offset before truncation.
			name: "PageBeyondCommit",
			delta: func(rng *rand.Rand) restoreTestFile {
				return restoreTestFile{level: 1, minTXID: 2, maxTXID: 2, commit: 100, pages: restoreTestPages(rng, 7)}
			},
			mutate: func(i int, b []byte) []byte {
				if i == 1 {
					return patchPageNumber(b, 0, 101)
				}
				return b
			},
			want: "page 101 beyond commit size 100",
		},
		{
			// Stored as 2-2 but its header claims 3-3.
			name: "TXIDGap",
			delta: func(rng *rand.Rand) restoreTestFile {
				return restoreTestFile{level: 1, minTXID: 3, maxTXID: 3, commit: 100, pages: restoreTestPages(rng, 7)}
			},
			want: "non-contiguous transaction ids",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(2))
			files := []restoreTestFile{snapshot(rng), tt.delta(rng)}

			client := file.NewReplicaClient(t.TempDir())
			for i, tf := range files {
				b := tf.encode(t)
				if tt.mutate != nil {
					b = tt.mutate(i, b)
				}
				minTXID, maxTXID := tf.minTXID, tf.maxTXID
				if tt.name == "TXIDGap" && i == 1 {
					minTXID, maxTXID = 2, 2
				}
				if _, err := client.WriteLTXFile(context.Background(), tf.level, minTXID, maxTXID, bytes.NewReader(b)); err != nil {
					t.Fatal(err)
				}
			}
			r := litestream.NewReplicaWithClient(nil, client)

			outputPath := filepath.Join(t.TempDir(), "restored.db")
			err := r.Restore(context.Background(), litestream.RestoreOptions{OutputPath: outputPath})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error containing %q, got %v", tt.want, err)
			}
			for _, path := range []string{outputPath, outputPath + ".tmp"} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("expected %s to not exist after a failed restore, err=%v", path, err)
				}
			}
		})
	}
}
