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
// file. Each file keeps its level (see checkForkPlan).
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
	client ReplicaClient
	infos  []*ltx.FileInfo
	pws    []*io.PipeWriter
	cancel context.CancelFunc
	g      *errgroup.Group
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
		client: opt.Fork.Client,
		infos:  infos,
		pws:    make([]*io.PipeWriter, len(infos)),
		cancel: cancel,
		g:      g,
	}
	for i, info := range infos {
		pr, pw := io.Pipe()
		s.pws[i] = pw
		g.Go(func() error {
			_, err := s.client.WriteLTXFile(ctx, info.Level, info.MinTXID, info.MaxTXID, pr)
			if err != nil {
				err = fmt.Errorf("copy L%d %s: %w", info.Level, ltx.FormatFilename(info.MinTXID, info.MaxTXID), err)
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
	txID := s.infos[len(s.infos)-1].MaxTXID + 1

	dirInfo, err := os.Stat(filepath.Dir(db.Path()))
	if err != nil {
		return 0, err
	}
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

// checkForkPlan returns ErrForkIneligible unless every restore-plan file can
// be copied to the fork replica at its own level, followed by the anchor
// [h+1, h+1] in L0.
//
// Compaction into each level seeks from max(level max, L9 max)+1 and reads
// the level below from there. For the copied levels to compact without gaps,
// the plan must start with a snapshot, have no overlapping files, and never
// return to a higher level once it has left it: each level then holds one
// contiguous run of plan files, which ends before the runs of every lower
// level, and the anchor follows the last of them. In particular, no file of
// the plan contains h+1, so the anchor overlaps nothing.
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
		case info.MinTXID != prev.MaxTXID+1:
			return fmt.Errorf("%w: L%d %s does not start right after %s",
				ErrForkIneligible, info.Level, name, prev.MaxTXID)
		case i > 1 && info.Level > prev.Level:
			return fmt.Errorf("%w: L%d %s follows L%d",
				ErrForkIneligible, info.Level, name, prev.Level)
		}
	}
	return nil
}
