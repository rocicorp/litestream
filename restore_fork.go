package litestream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"
)

// A forked restore copies the restore plan's LTX files, byte for byte, into a
// second, empty replica (B) while restoring from the source replica (A). B is
// a fork of A's backup: it inherits A's history up to the restored TXID h,
// and the two timelines only diverge after h. Copying whole files is pure I/O
// because an LTX file's checksums and page index cover only bytes inside the
// file. The files are laid out in B as one chain whose levels never increase,
// with overlapping files renamed (see forkTargets).
//
// The restore then seeds local state for an empty transaction h+1 and
// uploads it as B's level-0 head [h+1, h+1], from which an unmodified
// replicate resumes incrementally against B instead of writing a full base
// snapshot (see seedLocalState).

// ErrForkIneligible is returned (wrapped) when a restore plan cannot be used to
// fork a replica. Callers fall back to a plain restore.
var ErrForkIneligible = errors.New("restore plan cannot fork replica")

// RestoreFork configures a forked restore. The restore must be to the latest
// TXID and into the path of the DB the replica belongs to, whose meta
// directory receives the local anchor.
type RestoreFork struct {
	// Client is the replica to fork into. It must be empty; replicate must use it
	// as the database's replica.
	Client ReplicaClient

	// Forked reports, after Restore returns successfully, whether the restore
	// forked into Client. A plan that cannot fork is restored without
	// forking, leaving Client untouched.
	Forked bool

	// TXID is the TXID that the fork replica and the local anchor end at: the
	// empty transaction after the restored TXID.
	TXID ltx.TXID
}

// restoreForker copies restore-plan files into the fork replica as the
// restore reads them.
type restoreForker struct {
	client  ReplicaClient
	targets []ltx.FileInfo // where each plan file is copied (see forkTargets)
	pws     []*io.PipeWriter
	cancel  context.CancelFunc
	g       *errgroup.Group
}

// prepareFork returns a forker for infos, or nil when the plan cannot fork the
// replica.
func (r *Replica) prepareFork(ctx context.Context, opt RestoreOptions, infos []*ltx.FileInfo) (*restoreForker, error) {
	switch db := r.DB(); {
	case opt.Fork.Client == nil:
		return nil, fmt.Errorf("fork: replica client required")
	case db == nil:
		return nil, fmt.Errorf("fork: restore must belong to a database")
	case opt.OutputPath != db.Path():
		return nil, fmt.Errorf("fork: output path %s is not the database path %s", opt.OutputPath, db.Path())
	case opt.TXID != 0 || !opt.Timestamp.IsZero() || opt.Follow:
		return nil, fmt.Errorf("fork: only a restore to the latest TXID can fork a replica")
	}

	err := checkForkPlan(infos)
	if err == nil {
		err = checkForkEmpty(ctx, opt.Fork.Client)
	}
	if errors.Is(err, ErrForkIneligible) {
		r.Logger().Warn("restoring without forking", "reason", err)
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("fork: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)
	s := &restoreForker{
		client:  opt.Fork.Client,
		targets: forkTargets(infos),
		pws:     make([]*io.PipeWriter, len(infos)),
		cancel:  cancel,
		g:       g,
	}
	for i, target := range s.targets {
		pr, pw := io.Pipe()
		s.pws[i] = pw
		g.Go(func() error {
			_, err := s.client.WriteLTXFile(ctx, target.Level, target.MinTXID, target.MaxTXID, pr)
			if err != nil {
				info := infos[i]
				err = fmt.Errorf("copy L%d %s to L%d %s: %w",
					info.Level, ltx.FormatFilename(info.MinTXID, info.MaxTXID),
					target.Level, ltx.FormatFilename(target.MinTXID, target.MaxTXID), err)
			}
			_ = pr.CloseWithError(err)
			return err
		})
	}
	return s, nil
}

// checkForkEmpty returns ErrForkIneligible if client holds any LTX file.
func checkForkEmpty(ctx context.Context, client ReplicaClient) error {
	for level := 0; level <= SnapshotLevel; level++ {
		itr, err := client.LTXFiles(ctx, level, 0, false)
		if err != nil {
			return fmt.Errorf("list fork replica L%d: %w", level, err)
		}
		found := itr.Next()
		var info ltx.FileInfo
		if found {
			info = *itr.Item()
		}
		if err := itr.Close(); err != nil {
			return fmt.Errorf("list fork replica L%d: %w", level, err)
		}
		if found {
			return fmt.Errorf("%w: fork replica is not empty: L%d has %s",
				ErrForkIneligible, level, ltx.FormatFilename(info.MinTXID, info.MaxTXID))
		}
	}
	return nil
}

// tee returns a reader of plan file i that also copies every byte it reads
// into the fork replica. The copy completes when rd reaches EOF.
func (s *restoreForker) tee(i int, rd io.Reader) io.Reader {
	return &forkTee{rd: rd, pw: s.pws[i]}
}

// abort stops all copies and waits for them to exit.
func (s *restoreForker) abort(cause error) {
	s.cancel()
	for _, pw := range s.pws {
		_ = pw.CloseWithError(cause)
	}
	_ = s.g.Wait()
}

// wait waits for every copy to complete.
func (s *restoreForker) wait() error {
	defer s.cancel()
	return s.g.Wait()
}

// finish seeds the local state for the empty transaction h+1 after the
// restored TXID h, uploads its anchor as the fork replica's level-0 head, and
// returns h+1. Replicate takes its resume position from remote L0 only, so
// B's L0 must reach the anchor; it is uploaded only after every plan file has
// been copied, so that B never holds it without the history before it.
func (s *restoreForker) finish(ctx context.Context, db *DB) (ltx.TXID, error) {
	txID := s.targets[len(s.targets)-1].MaxTXID + 1

	dirInfo, err := os.Stat(filepath.Dir(db.Path()))
	if err != nil {
		return 0, err
	}
	defer db.invalidatePosCache() // the local L0 directory is replaced
	anchorPath, err := seedLocalState(ctx, db.Path(), db.LTXLevelDir(0), dirInfo, txID)
	if err != nil {
		return 0, err
	}
	if err := uploadForkAnchor(ctx, s.client, anchorPath, txID); err != nil {
		_ = os.Remove(anchorPath)
		return 0, err
	}
	return txID, nil
}

func uploadForkAnchor(ctx context.Context, client ReplicaClient, path string, txID ltx.TXID) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if _, err := client.WriteLTXFile(ctx, 0, txID, txID, f); err != nil {
		return fmt.Errorf("upload anchor to fork replica: %w", err)
	}
	return nil
}

// forkTee copies everything read from rd into pw, closing pw at EOF.
type forkTee struct {
	rd io.Reader
	pw *io.PipeWriter
}

func (t *forkTee) Read(p []byte) (int, error) {
	n, err := t.rd.Read(p)
	if n > 0 {
		if _, werr := t.pw.Write(p[:n]); werr != nil {
			return n, fmt.Errorf("fork copy: %w", werr)
		}
	}
	switch {
	case err == io.EOF:
		_ = t.pw.Close()
	case err != nil:
		_ = t.pw.CloseWithError(err)
	}
	return n, err
}

// checkForkPlan returns ErrForkIneligible unless infos is a restorable
// chain that starts with a snapshot: an L9 file [1,s] followed by files that
// each start no later than right after their predecessor and end past it.
// Any such plan can fork, because forkTargets lays it out in a shape that
// compaction accepts.
func checkForkPlan(infos []*ltx.FileInfo) error {
	if len(infos) == 0 {
		return fmt.Errorf("%w: empty restore plan", ErrForkIneligible)
	}
	if first := infos[0]; first.Level != SnapshotLevel || first.MinTXID != 1 {
		return fmt.Errorf("%w: plan starts with L%d %s, not a snapshot",
			ErrForkIneligible, first.Level, ltx.FormatFilename(first.MinTXID, first.MaxTXID))
	}
	for i := 1; i < len(infos); i++ {
		prev, info := infos[i-1], infos[i]
		name := ltx.FormatFilename(info.MinTXID, info.MaxTXID)
		switch {
		case info.Level == SnapshotLevel:
			return fmt.Errorf("%w: second snapshot %s in plan", ErrForkIneligible, name)
		case info.MinTXID > prev.MaxTXID+1:
			return fmt.Errorf("%w: L%d %s leaves a gap after %s",
				ErrForkIneligible, info.Level, name, prev.MaxTXID)
		case info.MaxTXID <= prev.MaxTXID:
			return fmt.Errorf("%w: L%d %s does not extend past %s",
				ErrForkIneligible, info.Level, name, prev.MaxTXID)
		}
	}
	return nil
}

// forkTargets returns the level and TXID range under which each file of a
// plan accepted by checkForkPlan is copied into the fork replica.
//
// Restore accepts a chain whose files come from any levels and may overlap,
// but compaction reads each level from the max+1 of the level above it, skips
// files that start before that point, and rejects inputs with a gap. Copied at
// their own levels, the files of one chain can leave a gap inside several
// levels. Instead:
//
//   - The snapshot keeps its level and name.
//   - Every other file goes to the highest level among itself and the files
//     after it, so that levels never increase along the chain. Only files
//     before a higher-level file move up: typically just the file that
//     straddles the snapshot.
//   - A file that overlaps its predecessor is renamed to start right after it.
//     Restore applies it to the same result: on top of the predecessor's
//     state, the pages it changed before that point already hold the values
//     it carries.
//
// The chain after the snapshot is then exactly [s+1, h], and each level holds
// one run of it with no gap or overlap, ending right where the runs of the
// lower levels begin. The anchor [h+1, h+1] follows in L0. Every level
// boundary is at s+1, at the end of a run, or at h+1, which is where
// compaction starts reading, so no file is ever skipped. Files keep their
// original levels where they can, so B goes on compacting the chain's tail as
// A would have.
//
// Renamed files are copied byte for byte, so their headers keep the original,
// wider TXID range. Nothing compares a header with its file name: restore and
// ltx.Compactor only check that each header's range is contiguous with what
// came before, which a wider range always is, and the pre-apply checksum,
// which no longer matches, is never verified. A check that a header matches
// its name would reject these files.
func forkTargets(infos []*ltx.FileInfo) []ltx.FileInfo {
	targets := make([]ltx.FileInfo, len(infos))
	targets[0] = ltx.FileInfo{Level: infos[0].Level, MinTXID: infos[0].MinTXID, MaxTXID: infos[0].MaxTXID}
	var level int
	for i := len(infos) - 1; i >= 1; i-- {
		level = max(level, infos[i].Level)
		targets[i] = ltx.FileInfo{
			Level:   level,
			MinTXID: max(infos[i].MinTXID, infos[i-1].MaxTXID+1),
			MaxTXID: infos[i].MaxTXID,
		}
	}
	return targets
}
