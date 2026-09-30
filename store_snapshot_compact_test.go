package litestream_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// snapshotCompactTest bundles a DB replicating to a file client with a SQL
// connection to it, for driving SnapshotModeCompact end to end.
type snapshotCompactTest struct {
	t      *testing.T
	ctx    context.Context
	db     *litestream.DB
	store  *litestream.Store
	client *file.ReplicaClient
	sqldb  *sql.DB
}

func newSnapshotCompactTest(t *testing.T) *snapshotCompactTest {
	t.Helper()
	ctx := context.Background()

	client := testingutil.NewFileReplicaClient(t)
	db := litestream.NewDB(filepath.Join(t.TempDir(), "db"))
	db.MonitorInterval = 0
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = client
	db.Replica.MonitorEnabled = false

	levels := litestream.CompactionLevels{{Level: 0}, {Level: 1, Interval: time.Second}, {Level: 2, Interval: time.Second}}
	store := litestream.NewStore([]*litestream.DB{db}, levels)
	store.CompactionMonitorEnabled = false
	store.SnapshotMode = litestream.SnapshotModeCompact
	if err := store.Open(ctx); err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(ctx); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	sqldb := testingutil.MustOpenSQLDB(t, db.Path())
	t.Cleanup(func() { testingutil.MustCloseSQLDB(t, sqldb) })
	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	return &snapshotCompactTest{t: t, ctx: ctx, db: db, store: store, client: client, sqldb: sqldb}
}

func (c *snapshotCompactTest) exec(query string, args ...any) {
	c.t.Helper()
	if _, err := c.sqldb.ExecContext(c.ctx, query, args...); err != nil {
		c.t.Fatalf("exec %q: %v", query, err)
	}
}

// insert adds n rows to t.
func (c *snapshotCompactTest) insert(n int) {
	c.t.Helper()
	for i := 0; i < n; i++ {
		c.exec(`INSERT INTO t (val) VALUES (?)`, fmt.Sprintf("value-%d", i))
	}
}

// sync writes the pending WAL to an LTX file and uploads it. The first sync
// writes the base, which the replica uploads to the snapshot level.
func (c *snapshotCompactTest) sync() {
	c.t.Helper()
	if err := c.db.Sync(c.ctx); err != nil {
		c.t.Fatalf("sync: %v", err)
	}
	if err := c.db.Replica.Sync(c.ctx); err != nil {
		c.t.Fatalf("replica sync: %v", err)
	}
}

func (c *snapshotCompactTest) snapshotCompact() *ltx.FileInfo {
	c.t.Helper()
	info, err := c.db.SnapshotCompact(c.ctx)
	if err != nil {
		c.t.Fatalf("snapshot compact: %v", err)
	}
	if info.Level != litestream.SnapshotLevel || info.MinTXID != 1 {
		c.t.Fatalf("expected a [1, k] snapshot, got level %d [%s, %s]", info.Level, info.MinTXID, info.MaxTXID)
	}
	return info
}

// restore restores the replica's head with a full integrity check and returns
// a connection to the restored database.
func (c *snapshotCompactTest) restore() *sql.DB {
	c.t.Helper()
	path := filepath.Join(c.t.TempDir(), "restore.db")
	if err := c.db.Replica.Restore(c.ctx, litestream.RestoreOptions{OutputPath: path, IntegrityCheck: litestream.IntegrityCheckFull}); err != nil {
		c.t.Fatalf("restore: %v", err)
	}
	restored := testingutil.MustOpenSQLDB(c.t, path)
	c.t.Cleanup(func() { testingutil.MustCloseSQLDB(c.t, restored) })
	return restored
}

func (c *snapshotCompactTest) remoteFiles(level int) []*ltx.FileInfo {
	c.t.Helper()
	infos, err := litestream.FindLTXFiles(c.ctx, c.client, level, false, func(*ltx.FileInfo) (bool, error) { return true, nil })
	if err != nil {
		c.t.Fatalf("list level %d: %v", level, err)
	}
	return infos
}

func countRows(t *testing.T, d *sql.DB) int {
	t.Helper()
	var n int
	if err := d.QueryRow(`SELECT count(*) FROM t`).Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return n
}

// decodeLTX decodes a full LTX snapshot stream to raw database bytes.
func decodeLTX(t *testing.T, r io.Reader) (ltx.Header, []byte) {
	t.Helper()
	dec := ltx.NewDecoder(r)
	var buf bytes.Buffer
	if err := dec.DecodeDatabaseTo(&buf); err != nil {
		t.Fatalf("decode database: %v", err)
	}
	return dec.Header(), buf.Bytes()
}

func (c *snapshotCompactTest) decodeRemote(info *ltx.FileInfo) (ltx.Header, []byte) {
	c.t.Helper()
	rc, err := c.client.OpenLTXFile(c.ctx, info.Level, info.MinTXID, info.MaxTXID, 0, 0)
	if err != nil {
		c.t.Fatalf("open ltx file: %v", err)
	}
	defer func() { _ = rc.Close() }()
	return decodeLTX(c.t, rc)
}

// TestStore_SnapshotCompact_RoundTrip verifies that a snapshot produced by
// SnapshotModeCompact (merging the prior snapshot + increments, never reading
// the live DB) restores to the same database state as the live data. The
// snapshot must be a valid [1, k] snapshot with zero WAL header fields: it is
// TXID-positioned, carrying no live-WAL extent.
func TestStore_SnapshotCompact_RoundTrip(t *testing.T) {
	t.Parallel()
	c := newSnapshotCompactTest(t)

	c.insert(256)
	c.sync()
	c.insert(256)
	c.sync()
	c.insert(256)
	c.sync()

	info := c.snapshotCompact()

	rc, err := c.client.OpenLTXFile(c.ctx, info.Level, info.MinTXID, info.MaxTXID, 0, 0)
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	defer func() { _ = rc.Close() }()
	dec := ltx.NewDecoder(rc)
	if err := dec.DecodeHeader(); err != nil {
		t.Fatalf("decode snapshot header: %v", err)
	}
	if h := dec.Header(); h.WALOffset != 0 || h.WALSize != 0 || h.WALSalt1 != 0 || h.WALSalt2 != 0 {
		t.Fatalf("expected zero WAL header fields, got offset=%d size=%d salt1=%d salt2=%d", h.WALOffset, h.WALSize, h.WALSalt1, h.WALSalt2)
	}

	if n := countRows(t, c.restore()); n != 768 {
		t.Fatalf("restored row count = %d, want 768", n)
	}
}

// TestStore_SnapshotCompact_NoIncrements verifies that with no increments on
// top of the current snapshot, there is nothing to do rather than rewriting an
// identical snapshot.
func TestStore_SnapshotCompact_NoIncrements(t *testing.T) {
	t.Parallel()
	c := newSnapshotCompactTest(t)
	c.sync()

	if _, err := c.db.SnapshotCompact(c.ctx); err != litestream.ErrNoCompaction {
		t.Fatalf("expected ErrNoCompaction, got %v", err)
	}
}

// TestStore_SnapshotCompact_MatchesLiveSnapshot verifies that a compaction
// snapshot decodes to exactly the same database bytes as a live snapshot taken
// at the same TXID.
func TestStore_SnapshotCompact_MatchesLiveSnapshot(t *testing.T) {
	t.Parallel()
	c := newSnapshotCompactTest(t)

	c.insert(200)
	c.sync()
	c.insert(200)
	c.exec(`UPDATE t SET val = 'updated' WHERE id % 3 = 0`)
	c.sync()
	c.exec(`DELETE FROM t WHERE id % 5 = 0`)
	c.sync()

	info := c.snapshotCompact()
	_, compacted := c.decodeRemote(info)

	pos, rc, err := c.db.SnapshotReader(c.ctx)
	if err != nil {
		t.Fatalf("live snapshot reader: %v", err)
	}
	defer func() { _ = rc.Close() }()
	if pos.TXID != info.MaxTXID {
		t.Fatalf("live snapshot at %s, compaction snapshot at %s", pos.TXID, info.MaxTXID)
	}
	_, live := decodeLTX(t, rc)

	if !bytes.Equal(compacted, live) {
		t.Fatalf("compaction snapshot (%d bytes) differs from live snapshot (%d bytes)", len(compacted), len(live))
	}
}

// TestStore_SnapshotCompact_MixedLevels verifies a merge whose plan spans the
// snapshot level and several compaction levels, not just L0.
func TestStore_SnapshotCompact_MixedLevels(t *testing.T) {
	t.Parallel()
	c := newSnapshotCompactTest(t)
	compact := func(level int) {
		t.Helper()
		if _, err := c.db.Compact(c.ctx, level); err != nil {
			t.Fatalf("compact L%d: %v", level, err)
		}
	}

	c.sync() // base -> L9
	c.insert(50)
	c.sync()
	c.insert(50)
	c.sync()
	compact(1)
	compact(2)
	c.insert(50)
	c.sync()
	c.insert(50)
	c.sync()
	compact(1)
	c.insert(50)
	c.sync()

	plan, err := litestream.CalcRestorePlan(c.ctx, c.client, 0, time.Time{}, c.db.Logger)
	if err != nil {
		t.Fatalf("calc restore plan: %v", err)
	}
	var levels []int
	for _, info := range plan {
		if !slices.Contains(levels, info.Level) {
			levels = append(levels, info.Level)
		}
	}
	for _, want := range []int{litestream.SnapshotLevel, 2, 1, 0} {
		if !slices.Contains(levels, want) {
			t.Fatalf("expected plan to include L%d, got levels %v", want, levels)
		}
	}

	info := c.snapshotCompact()
	if info.MaxTXID != plan[len(plan)-1].MaxTXID {
		t.Fatalf("snapshot at %s, want plan head %s", info.MaxTXID, plan[len(plan)-1].MaxTXID)
	}
	if n := countRows(t, c.restore()); n != 250 {
		t.Fatalf("restored row count = %d, want 250", n)
	}
}

// TestStore_SnapshotCompact_RestoreNeedsOnlyNewerFiles verifies the property
// that lets object-age lifecycle policies clean up the replica: once a
// compaction snapshot exists, deleting every file it covers (including earlier
// compaction snapshots it was chained from) leaves the replica restorable.
func TestStore_SnapshotCompact_RestoreNeedsOnlyNewerFiles(t *testing.T) {
	t.Parallel()
	c := newSnapshotCompactTest(t)

	c.insert(100)
	c.sync()
	c.insert(100)
	c.sync()
	c.snapshotCompact()
	c.insert(100)
	c.sync()
	snap := c.snapshotCompact() // seeded from the previous compaction snapshot
	c.insert(100)
	c.sync()

	var obsolete []*ltx.FileInfo
	for level := 0; level <= litestream.SnapshotLevel; level++ {
		for _, info := range c.remoteFiles(level) {
			if level == litestream.SnapshotLevel && info.MaxTXID < snap.MaxTXID {
				obsolete = append(obsolete, info)
			} else if level != litestream.SnapshotLevel && info.MaxTXID <= snap.MaxTXID {
				obsolete = append(obsolete, info)
			}
		}
	}
	if len(obsolete) == 0 {
		t.Fatal("expected files older than the newest snapshot")
	}
	if err := c.client.DeleteLTXFiles(c.ctx, obsolete); err != nil {
		t.Fatalf("delete obsolete files: %v", err)
	}

	if n := countRows(t, c.restore()); n != 400 {
		t.Fatalf("restored row count = %d, want 400", n)
	}
}

// TestStore_SnapshotCompact_Shrink verifies that pages past a smaller final
// database size are dropped when the database shrinks between inputs.
func TestStore_SnapshotCompact_Shrink(t *testing.T) {
	t.Parallel()
	c := newSnapshotCompactTest(t)

	c.exec(`CREATE TABLE big (data BLOB)`)
	for i := 0; i < 200; i++ {
		c.exec(`INSERT INTO big (data) VALUES (randomblob(4000))`)
	}
	c.insert(10)
	c.sync()
	base := c.remoteFiles(litestream.SnapshotLevel)[0]
	baseHdr, _ := c.decodeRemote(base)

	c.exec(`DROP TABLE big`)
	c.exec(`VACUUM`)
	c.sync()

	info := c.snapshotCompact()
	hdr, data := c.decodeRemote(info)
	if hdr.Commit >= baseHdr.Commit {
		t.Fatalf("expected the snapshot to shrink below %d pages, got %d", baseHdr.Commit, hdr.Commit)
	}
	if got, want := len(data), int(hdr.Commit)*int(hdr.PageSize); got != want {
		t.Fatalf("decoded %d bytes, want %d (commit %d)", got, want, hdr.Commit)
	}
	if n := countRows(t, c.restore()); n != 10 {
		t.Fatalf("restored row count = %d, want 10", n)
	}
}

// TestStore_SnapshotCompact_BaseOutsideSnapshotLevel verifies a replica whose
// base lives in L0 rather than the snapshot level (as written before the
// sync path uploaded bases to L9): the plan still starts at TXID 1, so the
// merge still produces a valid snapshot.
func TestStore_SnapshotCompact_BaseOutsideSnapshotLevel(t *testing.T) {
	t.Parallel()
	c := newSnapshotCompactTest(t)

	c.insert(100)
	c.sync()
	base := c.remoteFiles(litestream.SnapshotLevel)[0]
	rc, err := c.client.OpenLTXFile(c.ctx, base.Level, base.MinTXID, base.MaxTXID, 0, 0)
	if err != nil {
		t.Fatalf("open base: %v", err)
	}
	if _, err := c.client.WriteLTXFile(c.ctx, 0, base.MinTXID, base.MaxTXID, rc); err != nil {
		t.Fatalf("copy base to L0: %v", err)
	}
	_ = rc.Close()
	if err := c.client.DeleteLTXFiles(c.ctx, []*ltx.FileInfo{base}); err != nil {
		t.Fatalf("delete L9 base: %v", err)
	}
	c.insert(100)
	c.sync()

	info := c.snapshotCompact()
	if got := c.remoteFiles(litestream.SnapshotLevel); len(got) != 1 || got[0].MaxTXID != info.MaxTXID {
		t.Fatalf("expected the new snapshot to be the only L9 file, got %v", got)
	}
	if n := countRows(t, c.restore()); n != 200 {
		t.Fatalf("restored row count = %d, want 200", n)
	}
}

// TestStore_SnapshotCompact_NoBaseFallsBackToLive verifies that when no plan
// reconstructs the head (there is no base at TXID 1 anywhere), SnapshotCompact
// falls back to a live snapshot instead of failing every interval.
func TestStore_SnapshotCompact_NoBaseFallsBackToLive(t *testing.T) {
	t.Parallel()
	c := newSnapshotCompactTest(t)

	c.insert(100)
	c.sync()
	if err := c.client.DeleteLTXFiles(c.ctx, c.remoteFiles(litestream.SnapshotLevel)); err != nil {
		t.Fatalf("delete L9 base: %v", err)
	}
	c.insert(100)
	c.sync()

	info := c.snapshotCompact()
	pos, err := c.db.Pos()
	if err != nil {
		t.Fatalf("pos: %v", err)
	}
	if info.MaxTXID != pos.TXID {
		t.Fatalf("snapshot at %s, want live position %s", info.MaxTXID, pos.TXID)
	}
	if hdr, _ := c.decodeRemote(info); hdr.WALSalt1 == 0 && hdr.WALSalt2 == 0 {
		t.Fatal("expected a live (WAL-positioned) snapshot with WAL salts set")
	}
	if n := countRows(t, c.restore()); n != 200 {
		t.Fatalf("restored row count = %d, want 200", n)
	}
}

func TestParseSnapshotMode(t *testing.T) {
	for _, tt := range []struct {
		in      string
		want    litestream.SnapshotMode
		wantErr bool
	}{
		{in: "", want: litestream.SnapshotModeLive},
		{in: "live", want: litestream.SnapshotModeLive},
		{in: " Live ", want: litestream.SnapshotModeLive},
		{in: "compact", want: litestream.SnapshotModeCompact},
		{in: "COMPACTION", want: litestream.SnapshotModeCompact},
		{in: "bogus", wantErr: true},
	} {
		got, err := litestream.ParseSnapshotMode(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseSnapshotMode(%q): expected error", tt.in)
			}
			continue
		}
		if err != nil || got != tt.want {
			t.Errorf("ParseSnapshotMode(%q) = %v, %v; want %v", tt.in, got, err, tt.want)
		}
		if again, err := litestream.ParseSnapshotMode(got.String()); err != nil || again != got {
			t.Errorf("ParseSnapshotMode(%v.String()) = %v, %v; want round trip", got, again, err)
		}
	}
}
