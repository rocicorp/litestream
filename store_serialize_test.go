package litestream

import (
	"errors"
	"fmt"
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

// TestStore_CompactDB_SerializeModes verifies which levels take the per-DB lock
// under each CompactionSerialize mode. With the lock held by a simulated
// in-flight operation, a level that participates in serialization must be
// rejected with ErrCompactionInProgress; a level that does not must proceed
// past the guard (and stop later, here at the not-ready page-size check).
func TestStore_CompactDB_SerializeModes(t *testing.T) {
	tests := []struct {
		mode    CompactionSerializeMode
		level   int
		blocked bool // true => expect ErrCompactionInProgress while lock is held
	}{
		{SerializeAll, 1, true},
		{SerializeAll, SnapshotLevel, true},
		{SerializeHeavy, 1, false},
		{SerializeHeavy, SnapshotLevel, true},
		{SerializeOff, 1, false},
		{SerializeOff, SnapshotLevel, false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/L%d", tt.mode, tt.level), func(t *testing.T) {
			db := NewDB(filepath.Join(t.TempDir(), "db"))
			s := NewStore([]*DB{db}, CompactionLevels{{Level: 0}, {Level: 1}})
			s.CompactionSerialize = tt.mode
			lvl := &CompactionLevel{Level: tt.level}

			if !db.compactionMu.TryLock() {
				t.Fatal("precondition: expected to acquire compactionMu")
			}
			defer db.compactionMu.Unlock()

			_, err := s.CompactDB(t.Context(), db, lvl)
			if got := errors.Is(err, ErrCompactionInProgress); got != tt.blocked {
				t.Fatalf("mode=%s level=%d: in-progress=%v, want %v (err=%v)",
					tt.mode, tt.level, got, tt.blocked, err)
			}
		})
	}
}

func TestParseCompactionSerializeMode(t *testing.T) {
	valid := map[string]CompactionSerializeMode{
		"":        SerializeAll,
		"all":     SerializeAll,
		"ALL":     SerializeAll,
		" heavy ": SerializeHeavy,
		"heavy":   SerializeHeavy,
		"off":     SerializeOff,
		"none":    SerializeOff,
		"false":   SerializeOff,
	}
	for in, want := range valid {
		got, err := ParseCompactionSerializeMode(in)
		if err != nil {
			t.Fatalf("ParseCompactionSerializeMode(%q): unexpected error %v", in, err)
		}
		if got != want {
			t.Fatalf("ParseCompactionSerializeMode(%q) = %v, want %v", in, got, want)
		}
	}

	if _, err := ParseCompactionSerializeMode("bogus"); err == nil {
		t.Fatal("ParseCompactionSerializeMode(\"bogus\"): expected error, got nil")
	}
}
