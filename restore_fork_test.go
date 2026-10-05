package litestream_test

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// recordingClient records every WriteLTXFile call and can fail the next
// failN of them.
type recordingClient struct {
	litestream.ReplicaClient
	mu     sync.Mutex
	writes []ltx.FileInfo
	failN  int
}

func (c *recordingClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	c.mu.Lock()
	if c.failN > 0 {
		c.failN--
		c.mu.Unlock()
		return nil, errors.New("injected write failure")
	}
	c.writes = append(c.writes, ltx.FileInfo{Level: level, MinTXID: minTXID, MaxTXID: maxTXID})
	c.mu.Unlock()
	return c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
}

func (c *recordingClient) Writes() []ltx.FileInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ltx.FileInfo(nil), c.writes...)
}

func (c *recordingClient) SnapshotWrites() int {
	var n int
	for _, w := range c.Writes() {
		if w.Level == litestream.SnapshotLevel {
			n++
		}
	}
	return n
}

func forkCompactionLevels() litestream.CompactionLevels {
	return litestream.CompactionLevels{
		{Level: 0},
		{Level: 1, Interval: time.Nanosecond},
		{Level: 2, Interval: time.Nanosecond},
		{Level: 3, Interval: time.Nanosecond},
	}
}

// forkNode is a database replicating through a store with compaction levels
// L1-L3, compacted by hand.
type forkNode struct {
	t      *testing.T
	db     *litestream.DB
	sqldb  *sql.DB
	store  *litestream.Store
	levels litestream.CompactionLevels
}

// writeRows commits n transactions, each one TXID, and replicates them.
func (n *forkNode) writeRows(count int) {
	n.t.Helper()
	ctx := n.t.Context()
	for range count {
		tx, err := n.sqldb.BeginTx(ctx, nil)
		require.NoError(n.t, err)
		_, err = tx.ExecContext(ctx, `INSERT INTO t (b) VALUES (randomblob(1500))`)
		require.NoError(n.t, err)
		_, err = tx.ExecContext(ctx, `UPDATE t SET b = randomblob(1500) WHERE id = (SELECT min(id) FROM t)`)
		require.NoError(n.t, err)
		require.NoError(n.t, tx.Commit())
		n.sync()
	}
}

func (n *forkNode) sync() {
	n.t.Helper()
	require.NoError(n.t, n.db.Sync(n.t.Context()))
	require.NoError(n.t, n.db.Replica.Sync(n.t.Context()))
}

func (n *forkNode) compact(level int) {
	n.t.Helper()
	lvl := n.store.SnapshotLevel()
	if level != litestream.SnapshotLevel {
		lvl = n.levels[level]
	}
	_, err := n.store.CompactDB(n.t.Context(), n.db, lvl)
	require.NoError(n.t, err, "compact L%d", level)
}

func openStore(t *testing.T, db *litestream.DB, snapshotInterval time.Duration) (*litestream.Store, litestream.CompactionLevels) {
	t.Helper()
	levels := forkCompactionLevels()
	s := litestream.NewStore([]*litestream.DB{db}, levels)
	s.CompactionMonitorEnabled = false
	s.SnapshotInterval = snapshotInterval
	require.NoError(t, s.Open(t.Context()))
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s, levels
}

// newForkSource returns a source node whose base [1,1] is already at L9.
func newForkSource(t *testing.T) *forkNode {
	t.Helper()
	db, sqldb := testingutil.MustOpenDBs(t)
	t.Cleanup(func() { testingutil.MustCloseDBs(t, db, sqldb) })
	store, levels := openStore(t, db, time.Nanosecond)

	_, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INTEGER PRIMARY KEY, b BLOB)`)
	require.NoError(t, err)
	n := &forkNode{t: t, db: db, sqldb: sqldb, store: store, levels: levels}
	n.writeRows(1)
	return n
}

// forkedRestore is a database restored from a source replica by a forked
// restore into an empty fork replica.
type forkedRestore struct {
	path    string
	h       ltx.TXID
	clientB litestream.ReplicaClient
}

func forkFrom(t *testing.T, clientA litestream.ReplicaClient, wantHeadLevel int) *forkedRestore {
	t.Helper()
	ctx := t.Context()

	plan, err := litestream.CalcRestorePlan(ctx, clientA, 0, time.Time{}, slog.Default())
	require.NoError(t, err)
	head := plan[len(plan)-1]
	require.Equal(t, wantHeadLevel, head.Level, "plan head %s", ltx.FormatFilename(head.MinTXID, head.MaxTXID))

	path := filepath.Join(t.TempDir(), "db")
	clientB := testingutil.NewFileReplicaClient(t)
	fork := &litestream.RestoreFork{Client: clientB}
	require.NoError(t, litestream.NewReplicaWithClient(litestream.NewDB(path), clientA).Restore(ctx, litestream.RestoreOptions{
		OutputPath: path,
		Fork:       fork,
	}))
	require.True(t, fork.Forked)
	h := head.MaxTXID
	require.Equal(t, h+1, fork.TXID)

	// Every plan file is in B at its own level, followed by the empty
	// transaction [h+1,h+1] in L0, which is also the local anchor.
	for _, info := range plan {
		_, err := clientB.OpenLTXFile(ctx, info.Level, info.MinTXID, info.MaxTXID, 0, 0)
		require.NoError(t, err, "L%d %s", info.Level, ltx.FormatFilename(info.MinTXID, info.MaxTXID))
	}
	max, err := litestream.NewReplicaWithClient(nil, clientB).MaxLTXFileInfo(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, ltx.FileInfo{Level: 0, MinTXID: h + 1, MaxTXID: h + 1}, ltx.FileInfo{Level: max.Level, MinTXID: max.MinTXID, MaxTXID: max.MaxTXID}, "B's L0 head")
	_, err = os.Stat(litestream.NewDB(path).LTXPath(0, h+1, h+1))
	require.NoError(t, err, "expected the local anchor")

	return &forkedRestore{path: path, h: h, clientB: clientB}
}

// openForked starts an unmodified replicate of the forked database against
// the fork replica.
func openForked(t *testing.T, s *forkedRestore) (*forkNode, *recordingClient) {
	t.Helper()
	rec := &recordingClient{ReplicaClient: s.clientB}
	db := testingutil.NewDB(t, s.path)
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = rec
	db.Replica.MonitorEnabled = false
	store, levels := openStore(t, db, time.Hour)

	sqldb := testingutil.MustOpenSQLDB(t, s.path)
	t.Cleanup(func() { _ = sqldb.Close() })
	return &forkNode{t: t, db: db, sqldb: sqldb, store: store, levels: levels}, rec
}

func readRows(t *testing.T, sqldb *sql.DB) map[int64][]byte {
	t.Helper()
	rows, err := sqldb.QueryContext(t.Context(), `SELECT id, b FROM t ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()
	m := make(map[int64][]byte)
	for rows.Next() {
		var id int64
		var b []byte
		require.NoError(t, rows.Scan(&id, &b))
		m[id] = b
	}
	require.NoError(t, rows.Err())
	return m
}

// requireRestoresTo restores client into a fresh database and requires its
// rows to equal want's.
func requireRestoresTo(t *testing.T, client litestream.ReplicaClient, want *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db")
	require.NoError(t, litestream.NewReplicaWithClient(nil, client).Restore(t.Context(), litestream.RestoreOptions{
		OutputPath:     path,
		IntegrityCheck: litestream.IntegrityCheckFull,
	}))
	got := testingutil.MustOpenSQLDB(t, path)
	defer func() { _ = got.Close() }()
	require.Equal(t, readRows(t, want), readRows(t, got))
}

// requireIncrementalStart requires the forked node's first sync to continue
// after the anchor h+1, at h+2 into remote L0, with no snapshot.
func requireIncrementalStart(t *testing.T, n *forkNode, rec *recordingClient, h ltx.TXID) {
	t.Helper()
	require.NoError(t, n.db.Sync(t.Context()))
	pos, err := n.db.Pos()
	require.NoError(t, err)
	require.Equal(t, h+2, pos.TXID)
	_, err = os.Stat(n.db.LTXPath(0, h+2, h+2))
	require.NoError(t, err, "first sync must write an increment, not a snapshot")

	require.NoError(t, n.db.Replica.Sync(t.Context()))
	require.Equal(t, []ltx.FileInfo{{Level: 0, MinTXID: h + 2, MaxTXID: h + 2}}, rec.Writes())
}

func TestForkedRestore(t *testing.T) {
	for _, tc := range []struct {
		name      string
		build     func(*forkNode)
		headLevel int
	}{
		{
			// Plan: L9 [1,1], L2 [2,5], L1 [6,8], L0 [9,9], [10,10].
			name: "HeadL0",
			build: func(n *forkNode) {
				n.writeRows(4)
				n.compact(1)
				n.compact(2)
				n.writeRows(3)
				n.compact(1)
				n.writeRows(2)
			},
			headLevel: 0,
		},
		{
			// Plan: L9 [1,1], L3 [2,5], L2 [6,8]; the head goes to B's L0.
			name: "HeadL2",
			build: func(n *forkNode) {
				n.writeRows(4)
				n.compact(1)
				n.compact(2)
				n.compact(3)
				n.writeRows(3)
				n.compact(1)
				n.compact(2)
			},
			headLevel: 2,
		},
		{
			// Plan: L9 [1,5] only; the anchor is also uploaded to B's L0.
			name: "SnapshotOnly",
			build: func(n *forkNode) {
				n.writeRows(4)
				n.compact(litestream.SnapshotLevel)
			},
			headLevel: litestream.SnapshotLevel,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := newForkSource(t)
			tc.build(src)
			forked := forkFrom(t, src.db.Replica.Client, tc.headLevel)
			// B ends at the empty transaction h+1: the source's state at h.
			requireRestoresTo(t, forked.clientB, src.sqldb)

			n, rec := openForked(t, forked)
			requireIncrementalStart(t, n, rec, forked.h)

			// Keep replicating and run the whole compaction ladder on B.
			n.writeRows(3)
			for level := 1; level <= 3; level++ {
				n.compact(level)
			}
			n.writeRows(2)
			n.compact(1)
			n.compact(2)

			require.Zero(t, rec.SnapshotWrites(), "forked replicate must not snapshot")
			for level := 0; level <= 3; level++ {
				errs, err := n.db.Replica.ValidateLevel(t.Context(), level)
				require.NoError(t, err)
				require.Empty(t, errs, "L%d", level)
			}
			requireRestoresTo(t, forked.clientB, n.sqldb)
		})
	}
}

func headL2Source(t *testing.T) *forkedRestore {
	t.Helper()
	src := newForkSource(t)
	src.writeRows(4)
	src.compact(1)
	src.compact(2)
	src.compact(3)
	src.writeRows(3)
	src.compact(1)
	src.compact(2)
	return forkFrom(t, src.db.Replica.Client, 2)
}

func requireSnapshotFallback(t *testing.T, forked *forkedRestore) {
	t.Helper()
	n, rec := openForked(t, forked)
	n.writeRows(1)
	require.Equal(t, 1, rec.SnapshotWrites(), "a lost WAL salt must fall back to a snapshot")
	requireRestoresTo(t, forked.clientB, n.sqldb)
}

func TestForkedRestore_WALDeleted(t *testing.T) {
	forked := headL2Source(t)
	require.NoError(t, os.Remove(forked.path+"-wal"))
	_ = os.Remove(forked.path + "-shm")
	requireSnapshotFallback(t, forked)
}

func TestForkedRestore_LastConnectionClose(t *testing.T) {
	forked := headL2Source(t)

	// An ordinary connection (no PERSIST_WAL) that is the last to close
	// checkpoints and deletes the WAL.
	other := testingutil.MustOpenSQLDB(t, forked.path)
	var count int
	require.NoError(t, other.QueryRowContext(t.Context(), `SELECT count(*) FROM t`).Scan(&count))
	require.NoError(t, other.Close())
	_, err := os.Stat(forked.path + "-wal")
	require.True(t, os.IsNotExist(err), "expected WAL to be deleted, got %v", err)

	requireSnapshotFallback(t, forked)
}

func TestForkedRestore_KeeperConnection(t *testing.T) {
	forked := headL2Source(t)
	ctx := t.Context()

	// The keeper holds a SHARED lock, so no other connection's close can
	// checkpoint and delete the WAL.
	keeper, err := sql.Open("sqlite", forked.path)
	require.NoError(t, err)
	keeper.SetMaxOpenConns(1)
	var count int
	require.NoError(t, keeper.QueryRowContext(ctx, `SELECT count(*) FROM t`).Scan(&count))

	// Another connection writes and closes before replicate starts, as the
	// replication-manager's post-restore steps do.
	other := testingutil.MustOpenSQLDB(t, forked.path)
	_, err = other.ExecContext(ctx, `INSERT INTO t (b) VALUES (randomblob(1500))`)
	require.NoError(t, err)
	require.NoError(t, other.Close())

	n, rec := openForked(t, forked)
	requireIncrementalStart(t, n, rec, forked.h)
	require.NoError(t, keeper.Close())

	n.writeRows(2)
	require.Zero(t, rec.SnapshotWrites())
	requireRestoresTo(t, forked.clientB, n.sqldb)
}

func TestForkedRestore_LocalAnchorMissing(t *testing.T) {
	// Without the local anchor, checkDatabaseBehindReplica downloads B's L0
	// head [h+1,h+1], which records the stamped WAL salt: with the WAL intact,
	// replicate still resumes incrementally.
	forked := headL2Source(t)
	require.NoError(t, os.RemoveAll(litestream.NewDB(forked.path).LTXLevelDir(0)))

	n, rec := openForked(t, forked)
	requireIncrementalStart(t, n, rec, forked.h)

	n.writeRows(2)
	for level := 1; level <= 3; level++ {
		n.compact(level)
	}
	require.Zero(t, rec.SnapshotWrites())
	requireRestoresTo(t, forked.clientB, n.sqldb)
}

// The local anchor and B's L0 head [h+1,h+1] are the same file, so L0
// retention deletes the anchor along with that head.
func TestForkedRestore_AnchorDeletedWithRemoteHead(t *testing.T) {
	for name, loseLocalState := range map[string]bool{
		// The forked restore's own anchor.
		"Forked": false,
		// The copy checkDatabaseBehindReplica downloads.
		"BehindReplica": true,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			forked := headL2Source(t)
			if loseLocalState {
				require.NoError(t, os.RemoveAll(litestream.NewDB(forked.path).LTXLevelDir(0)))
			}
			n, _ := openForked(t, forked)
			require.NoError(t, n.db.Sync(ctx)) // runs checkDatabaseBehindReplica

			anchor := forked.h + 1
			anchorPath := n.db.LTXPath(0, anchor, anchor)
			_, err := os.Stat(anchorPath)
			require.NoError(t, err, "expected the local anchor")

			n.writeRows(2)
			n.compact(1)
			n.db.L0Retention = time.Nanosecond
			require.NoError(t, n.db.EnforceL0RetentionByTime(ctx))

			_, err = forked.clientB.OpenLTXFile(ctx, 0, anchor, anchor, 0, 0)
			require.ErrorIs(t, err, os.ErrNotExist, "expected retention to delete B's L0 head")
			_, err = os.Stat(anchorPath)
			require.ErrorIs(t, err, os.ErrNotExist, "expected retention to delete the local anchor")
			requireRestoresTo(t, forked.clientB, n.sqldb)
		})
	}
}

func TestForkedRestore_SyncErrorResumesFromRemoteL0(t *testing.T) {
	forked := headL2Source(t)
	n, rec := openForked(t, forked)

	require.NoError(t, n.db.Sync(t.Context()))
	rec.mu.Lock()
	rec.failN = 1
	rec.mu.Unlock()
	require.Error(t, n.db.Replica.Sync(t.Context()))
	require.True(t, n.db.Replica.Pos().IsZero(), "a sync error must reset the replica position")

	// The retry recomputes the position from B's remote L0, which reaches h+1.
	require.NoError(t, n.db.Replica.Sync(t.Context()))
	require.Equal(t, []ltx.FileInfo{{Level: 0, MinTXID: forked.h + 2, MaxTXID: forked.h + 2}}, rec.Writes())
	requireRestoresTo(t, forked.clientB, n.sqldb)
}

func TestCheckForkPlan(t *testing.T) {
	file := func(level int, minTXID, maxTXID ltx.TXID) *ltx.FileInfo {
		return &ltx.FileInfo{Level: level, MinTXID: minTXID, MaxTXID: maxTXID}
	}
	snap := file(litestream.SnapshotLevel, 1, 4)

	for _, tc := range []struct {
		name     string
		plan     []*ltx.FileInfo
		eligible bool
	}{
		{"SnapshotOnly", []*ltx.FileInfo{snap}, true},
		{"BaseOnly", []*ltx.FileInfo{file(litestream.SnapshotLevel, 1, 1)}, true},
		{"HeadL0", []*ltx.FileInfo{snap, file(2, 5, 8), file(1, 9, 10), file(0, 11, 11)}, true},
		{"HeadL2", []*ltx.FileInfo{snap, file(3, 5, 8), file(2, 9, 12)}, true},
		{"SameLevelRun", []*ltx.FileInfo{snap, file(1, 5, 8), file(1, 9, 12)}, true},
		{"Empty", nil, false},
		{"NoSnapshot", []*ltx.FileInfo{file(3, 1, 8)}, false},
		{"Overlap", []*ltx.FileInfo{snap, file(2, 3, 8)}, false},
		{"LevelIncreases", []*ltx.FileInfo{snap, file(1, 5, 8), file(2, 9, 12)}, false},
		{"SecondSnapshot", []*ltx.FileInfo{snap, file(litestream.SnapshotLevel, 5, 8)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := litestream.CheckForkPlan(tc.plan)
			if tc.eligible {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, litestream.ErrForkIneligible)
			}
		})
	}
}

func TestRestore_ForkIneligible(t *testing.T) {
	src := newForkSource(t)
	src.writeRows(3)
	ctx := t.Context()

	// A non-empty fork replica is ineligible; the restore goes ahead
	// without touching it or writing an anchor.
	clientB := testingutil.NewFileReplicaClient(t)
	rc, err := src.db.Replica.Client.OpenLTXFile(ctx, litestream.SnapshotLevel, 1, 1, 0, 0)
	require.NoError(t, err)
	_, err = clientB.WriteLTXFile(ctx, litestream.SnapshotLevel, 1, 1, rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	path := filepath.Join(t.TempDir(), "db")
	db := litestream.NewDB(path)
	fork := &litestream.RestoreFork{Client: clientB}
	require.NoError(t, litestream.NewReplicaWithClient(db, src.db.Replica.Client).Restore(ctx, litestream.RestoreOptions{
		OutputPath: path,
		Fork:       fork,
	}))
	require.False(t, fork.Forked)

	_, err = os.Stat(db.LTXLevelDir(0))
	require.True(t, os.IsNotExist(err), "no anchor may be written, got %v", err)
	info, err := litestream.NewReplicaWithClient(nil, clientB).MaxLTXFileInfo(ctx, 0)
	require.NoError(t, err)
	require.Zero(t, info.MaxTXID, "fork replica must be untouched")
}

func TestRestore_ForkUploadFailure(t *testing.T) {
	src := newForkSource(t)
	src.writeRows(3)
	src.compact(1)
	src.writeRows(2)
	ctx := t.Context()

	// The second copy fails: the restore fails and leaves no anchor.
	path := filepath.Join(t.TempDir(), "db")
	db := litestream.NewDB(path)
	r := litestream.NewReplicaWithClient(db, src.db.Replica.Client)

	failing := &failNthWriteClient{ReplicaClient: testingutil.NewFileReplicaClient(t), n: 2}
	fork := &litestream.RestoreFork{Client: failing}
	err := r.Restore(ctx, litestream.RestoreOptions{OutputPath: path, Fork: fork})
	require.ErrorContains(t, err, "injected write failure")
	require.False(t, fork.Forked)

	_, err = os.Stat(db.LTXLevelDir(0))
	require.True(t, os.IsNotExist(err), "no anchor may be written, got %v", err)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "no database may be left, got %v", err)
}

func TestRestore_ForkAnchorUploadFailure(t *testing.T) {
	src := newForkSource(t)
	src.writeRows(3)
	ctx := t.Context()
	plan, err := litestream.CalcRestorePlan(ctx, src.db.Replica.Client, 0, time.Time{}, slog.Default())
	require.NoError(t, err)

	// Every plan file is copied; the anchor upload after them fails. The
	// restore fails and leaves no local anchor.
	path := filepath.Join(t.TempDir(), "db")
	db := litestream.NewDB(path)
	r := litestream.NewReplicaWithClient(db, src.db.Replica.Client)
	failing := &failNthWriteClient{ReplicaClient: testingutil.NewFileReplicaClient(t), n: len(plan) + 1}
	fork := &litestream.RestoreFork{Client: failing}
	err = r.Restore(ctx, litestream.RestoreOptions{OutputPath: path, Fork: fork})
	require.ErrorContains(t, err, "injected write failure")
	require.False(t, fork.Forked)

	h := plan[len(plan)-1].MaxTXID
	_, err = os.Stat(db.LTXPath(0, h+1, h+1))
	require.True(t, os.IsNotExist(err), "no anchor may be left, got %v", err)
}

func TestRestore_ForkRequiresLatestIntoDBPath(t *testing.T) {
	src := newForkSource(t)
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "db")
	fork := &litestream.RestoreFork{Client: testingutil.NewFileReplicaClient(t)}

	for name, tc := range map[string]struct {
		db  *litestream.DB
		opt litestream.RestoreOptions
	}{
		"NoDB":      {nil, litestream.RestoreOptions{OutputPath: path, Fork: fork}},
		"OtherPath": {litestream.NewDB(path + "2"), litestream.RestoreOptions{OutputPath: path, Fork: fork}},
		"TXID":      {litestream.NewDB(path), litestream.RestoreOptions{OutputPath: path, Fork: fork, TXID: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			err := litestream.NewReplicaWithClient(tc.db, src.db.Replica.Client).Restore(ctx, tc.opt)
			require.ErrorContains(t, err, "fork:")
		})
	}
}

// failNthWriteClient fails the nth WriteLTXFile call, after reading part of
// its input.
type failNthWriteClient struct {
	litestream.ReplicaClient
	mu sync.Mutex
	i  int
	n  int
}

func (c *failNthWriteClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	c.mu.Lock()
	c.i++
	fail := c.i == c.n
	c.mu.Unlock()
	if fail {
		_, _ = io.ReadFull(r, make([]byte, ltx.HeaderSize))
		return nil, errors.New("injected write failure")
	}
	return c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
}
