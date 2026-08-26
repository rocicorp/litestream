package litestream

import (
	"errors"
	"path/filepath"
	"testing"
)

// TestStore_CompactDB_SerializesPerDB verifies that CompactDB refuses to run a
// second compaction/snapshot for a database while one is already in flight.
// The snapshot level and each compaction level run in independent goroutines,
// so without this guard a snapshot (L9) and a lower-level compaction could run
// concurrently for the same DB and each retain a database-sized page index,
// overlapping enough to OOM a large database.
func TestStore_CompactDB_SerializesPerDB(t *testing.T) {
	db := NewDB(filepath.Join(t.TempDir(), "db"))
	s := NewStore([]*DB{db}, CompactionLevels{{Level: 0}, {Level: 1}})
	lvl := &CompactionLevel{Level: 1}

	// Simulate an in-flight compaction/snapshot holding the per-DB lock.
	if !db.compactionMu.TryLock() {
		t.Fatal("precondition: expected to acquire compactionMu")
	}

	// A concurrent CompactDB for the same DB must skip rather than run alongside.
	if _, err := s.CompactDB(t.Context(), db, lvl); !errors.Is(err, ErrCompactionInProgress) {
		t.Fatalf("CompactDB() while in progress: error = %v, want ErrCompactionInProgress", err)
	}

	// Once the in-flight compaction releases, CompactDB proceeds past the guard
	// (here it stops later at the not-ready check, but the point is that it is
	// no longer rejected as in-progress).
	db.compactionMu.Unlock()
	if _, err := s.CompactDB(t.Context(), db, lvl); errors.Is(err, ErrCompactionInProgress) {
		t.Fatalf("CompactDB() after release still reports in-progress: %v", err)
	}
}
