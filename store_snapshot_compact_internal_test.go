package litestream

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

// backdateSnapshot makes the cached newest snapshot look older than one
// snapshot interval, so CompactDB's ErrCompactionTooEarly gate lets the
// snapshot level through without the test waiting out an interval.
func backdateSnapshot(t *testing.T, db *DB) {
	t.Helper()
	info, err := db.MaxLTXFileInfo(t.Context(), SnapshotLevel)
	if err != nil {
		t.Fatal(err)
	}
	info.CreatedAt = time.Now().Add(-48 * time.Hour)
	db.recordMaxLTXFile(SnapshotLevel, &info)
}

func litestreamSeq(t *testing.T, db *DB) int {
	t.Helper()
	sqldb, err := sql.Open("sqlite", db.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()
	var seq int
	if err := sqldb.QueryRow(`SELECT COALESCE(MAX(seq), 0) FROM _litestream_seq`).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	return seq
}

// TestStore_CompactDB_RefreshesIdleSnapshot verifies that in compact mode an
// idle database still gets a new snapshot once the newest one is an interval
// old, so object-age lifecycle policies never expire the only snapshot. The
// first tick writes a real transaction and reports ErrSnapshotPending; once
// that write is replicated, the retry snapshots it under a new name.
func TestStore_CompactDB_RefreshesIdleSnapshot(t *testing.T) {
	for _, tt := range []struct {
		mode    SnapshotMode
		refresh bool
	}{
		{mode: SnapshotModeCompact, refresh: true},
		{mode: SnapshotModeLive, refresh: false},
	} {
		t.Run(tt.mode.String(), func(t *testing.T) {
			db, _ := openSpillTestDB(t, false)
			syncSnapshotCompactTestDB(t, db)
			prev, err := db.SnapshotCompact(t.Context()) // snapshot at the head: now idle
			if err != nil {
				t.Fatal(err)
			}
			backdateSnapshot(t, db)
			seqBefore := litestreamSeq(t, db)

			s := NewStore([]*DB{db}, CompactionLevels{{Level: 0}, {Level: 1, Interval: time.Hour}})
			s.SnapshotMode = tt.mode
			info, err := s.CompactDB(t.Context(), db, s.SnapshotLevel())

			if !tt.refresh {
				if !errors.Is(err, ErrNoCompaction) {
					t.Fatalf("CompactDB() error = %v, want ErrNoCompaction", err)
				}
				if seq := litestreamSeq(t, db); seq != seqBefore {
					t.Fatalf("_litestream_seq changed from %d to %d without a refresh", seqBefore, seq)
				}
				return
			}
			if !errors.Is(err, ErrSnapshotPending) {
				t.Fatalf("CompactDB() error = %v, want ErrSnapshotPending", err)
			}
			if seq := litestreamSeq(t, db); seq <= seqBefore {
				t.Fatalf("_litestream_seq = %d, want it bumped past %d", seq, seqBefore)
			}

			// The monitors would replicate the write before the retry.
			if err := db.Sync(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := db.Replica.Sync(t.Context()); err != nil {
				t.Fatal(err)
			}
			info, err = s.CompactDB(t.Context(), db, s.SnapshotLevel())
			if err != nil {
				t.Fatalf("CompactDB() retry error = %v", err)
			}
			if info.MinTXID != 1 || info.MaxTXID != prev.MaxTXID+1 {
				t.Fatalf("refreshed snapshot = [%s, %s], want [1, %s]", info.MinTXID, info.MaxTXID, prev.MaxTXID+1)
			}
		})
	}
}

// TestStore_CompactDB_SnapshotPendingReplication verifies that a compact-mode
// snapshot tick that finds the database ahead of the replica reports
// ErrSnapshotPending (retried soon) rather than ErrNoCompaction (which would
// wait a full interval).
func TestStore_CompactDB_SnapshotPendingReplication(t *testing.T) {
	db, _ := openSpillTestDB(t, false)
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.Replica.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Write and capture an increment locally, but do not upload it.
	sqldb, err := sql.Open("sqlite", db.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()
	if _, err := sqldb.Exec(`INSERT INTO t(data) VALUES (randomblob(3000));`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	backdateSnapshot(t, db)

	s := NewStore([]*DB{db}, CompactionLevels{{Level: 0}, {Level: 1, Interval: time.Hour}})
	s.SnapshotMode = SnapshotModeCompact
	if _, err := s.CompactDB(t.Context(), db, s.SnapshotLevel()); !errors.Is(err, ErrSnapshotPending) {
		t.Fatalf("CompactDB() error = %v, want ErrSnapshotPending", err)
	}
}

// failingSnapshotClient fails the first N snapshot-level uploads.
type failingSnapshotClient struct {
	*testReplicaClient
	failures atomic.Int32
}

func (c *failingSnapshotClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	if level == SnapshotLevel && c.failures.Add(-1) >= 0 {
		return nil, errors.New("injected snapshot upload failure")
	}
	return c.testReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
}

// TestStore_MonitorSnapshotLevel_RetriesFailure verifies that a failed
// snapshot is retried after SnapshotRetryInterval rather than a full snapshot
// interval later.
func TestStore_MonitorSnapshotLevel_RetriesFailure(t *testing.T) {
	db, _ := openSpillTestDB(t, false)
	syncSnapshotCompactTestDB(t, db)
	pos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}

	client := &failingSnapshotClient{testReplicaClient: db.Replica.Client.(*testReplicaClient)}
	client.failures.Store(2)
	db.Replica.Client = client
	backdateSnapshot(t, db)

	s := NewStore([]*DB{db}, CompactionLevels{{Level: 0}, {Level: 1, Interval: time.Hour}})
	s.SnapshotMode = SnapshotModeCompact
	s.SnapshotInterval = time.Hour
	s.SnapshotRetryInterval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.monitorCompactionLevel(ctx, s.SnapshotLevel())
	}()
	defer func() { cancel(); <-done }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		info, err := db.MaxLTXFileInfo(t.Context(), SnapshotLevel)
		if err != nil {
			t.Fatal(err)
		}
		if info.MaxTXID == pos.TXID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot not retried: newest snapshot at %s, want %s (failures left %d)", info.MaxTXID, pos.TXID, client.failures.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := client.failures.Load(); n >= 0 {
		t.Fatalf("expected both injected failures to be consumed before success, %d left", n)
	}
}

func TestNextSnapshotRetryDelay(t *testing.T) {
	for _, tt := range []struct {
		name               string
		prev, initial, max time.Duration
		want               time.Duration
	}{
		{name: "first failure", prev: 0, initial: time.Minute, max: time.Hour, want: time.Minute},
		{name: "doubles", prev: time.Minute, initial: time.Minute, max: time.Hour, want: 2 * time.Minute},
		{name: "capped at interval", prev: 40 * time.Minute, initial: time.Minute, max: time.Hour, want: time.Hour},
		{name: "disabled", prev: 0, initial: 0, max: time.Hour, want: time.Hour},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := nextSnapshotRetryDelay(tt.prev, tt.initial, tt.max); got != tt.want {
				t.Fatalf("nextSnapshotRetryDelay(%v, %v, %v) = %v, want %v", tt.prev, tt.initial, tt.max, got, tt.want)
			}
		})
	}
}
