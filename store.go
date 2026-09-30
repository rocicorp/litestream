package litestream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"
)

var (
	// ErrNoCompaction is returned when no new files are available from the previous level.
	ErrNoCompaction = errors.New("no compaction")

	// ErrCompactionTooEarly is returned when a compaction is attempted too soon
	// since the last compaction time. This is used to prevent frequent
	// re-compaction when restarting the process.
	ErrCompactionTooEarly = errors.New("compaction too early")

	// ErrCompactionInProgress is returned when a compaction or snapshot is
	// already running for the same database. Compaction levels and the snapshot
	// level run in independent goroutines; without this guard a snapshot (L9)
	// and a lower-level compaction can run concurrently for one database, each
	// retaining a database-sized in-memory page index. On a large database that
	// overlap can exhaust the memory limit and trigger an OOM kill. The monitor
	// treats this like ErrNoCompaction and retries on its next interval.
	ErrCompactionInProgress = errors.New("compaction in progress")

	// ErrSnapshotPending is returned by a compact-mode snapshot when the
	// database has advanced past the newest snapshot but the replica does not
	// yet hold the increments to fold in (including the _litestream_seq write
	// that refreshes an idle database). The monitor retries it after
	// SnapshotRetryInterval rather than waiting a full snapshot interval.
	ErrSnapshotPending = errors.New("snapshot pending replication")

	// ErrTxNotAvailable is returned when a transaction does not exist.
	ErrTxNotAvailable = errors.New("transaction not available")

	// ErrDBNotReady is a sentinel for errors.Is() compatibility.
	ErrDBNotReady = &DBNotReadyError{}

	// ErrShutdownInterrupted is returned when the shutdown sync retry loop
	// is interrupted by a done channel signal (e.g., second Ctrl+C).
	ErrShutdownInterrupted = errors.New("shutdown sync interrupted")

	ErrDatabaseNotFound = errors.New("database not found")
	ErrDatabaseNotOpen  = errors.New("database not open")
)

// CompactionSerializeMode controls how per-database compaction/snapshot
// serialization is applied in Store.CompactDB. It exists so the behavior can
// be compared at runtime (see the LITESTREAM_COMPACTION_SERIALIZE env var).
type CompactionSerializeMode int

const (
	// SerializeAll serializes every level (L1..L9) against each other per DB.
	// This is the historical behavior: at most one compaction or snapshot runs
	// for a given database at a time. Safest for memory, but a long-running
	// snapshot (L9) blocks lower-level compactions for its whole duration.
	SerializeAll CompactionSerializeMode = iota

	// SerializeHeavy serializes only the snapshot level (L9), which is the only
	// O(db-size) operation once lower levels seek past the newest snapshot.
	// Incremental compactions (L1..) run freely, so they are not starved while a
	// snapshot is in flight, while the whole-DB page-index memory footprint is
	// still capped at one snapshot at a time.
	SerializeHeavy

	// SerializeOff disables per-DB compaction serialization entirely.
	SerializeOff
)

// ParseCompactionSerializeMode parses a mode from its string form
// ("all", "heavy", "off"). An empty string returns SerializeAll (the default).
// Unrecognized values return an error.
func ParseCompactionSerializeMode(s string) (CompactionSerializeMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "all":
		return SerializeAll, nil
	case "heavy":
		return SerializeHeavy, nil
	case "off", "none", "false":
		return SerializeOff, nil
	default:
		return SerializeAll, fmt.Errorf("invalid compaction serialize mode %q (want all, heavy, or off)", s)
	}
}

// String returns the canonical string form of the mode.
func (m CompactionSerializeMode) String() string {
	switch m {
	case SerializeAll:
		return "all"
	case SerializeHeavy:
		return "heavy"
	case SerializeOff:
		return "off"
	default:
		return fmt.Sprintf("CompactionSerializeMode(%d)", int(m))
	}
}

// SnapshotMode selects how periodic snapshots (L9) are produced.
type SnapshotMode int

const (
	// SnapshotModeLive reads the current database file + WAL under the checkpoint
	// read lock and encodes a fresh full-DB image. This is the historical
	// behavior: it captures a WAL-positioned point-in-time cut, but holds
	// chkMu.RLock for the whole read, which stalls checkpoints on hot databases.
	SnapshotModeLive SnapshotMode = iota

	// SnapshotModeCompact mints each snapshot by merging the existing remote LTX
	// files (the prior snapshot plus the increments on top of it, as chosen by
	// the restore planner) into a new snapshot. It never reads the live database
	// and takes no checkpoint lock, at the cost of reading the prior snapshot
	// back from the replica. The result is TXID-positioned (no live WAL), so the
	// snapshot's WAL header fields are left zero.
	//
	// Because every restore then depends only on files written within roughly
	// one snapshot interval, this mode also refreshes the snapshot of an idle
	// database (see Store.CompactDB), so object-age lifecycle policies on the
	// replica never expire the only snapshot.
	SnapshotModeCompact
)

// ParseSnapshotMode parses a mode from its string form ("live", "compact").
// An empty string returns SnapshotModeLive (the default). Unrecognized values
// return an error.
func ParseSnapshotMode(s string) (SnapshotMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "live":
		return SnapshotModeLive, nil
	case "compact", "compaction":
		return SnapshotModeCompact, nil
	default:
		return SnapshotModeLive, fmt.Errorf("invalid snapshot mode %q (want live or compact)", s)
	}
}

// String returns the canonical string form of the mode.
func (m SnapshotMode) String() string {
	switch m {
	case SnapshotModeLive:
		return "live"
	case SnapshotModeCompact:
		return "compact"
	default:
		return fmt.Sprintf("SnapshotMode(%d)", int(m))
	}
}

// DBNotReadyError is returned when an operation is attempted before the
// database has been initialized (e.g., page size not yet known).
type DBNotReadyError struct {
	Reason string
}

func (e *DBNotReadyError) Error() string {
	if e.Reason != "" {
		return "db not ready: " + e.Reason
	}
	return "db not ready"
}

func (e *DBNotReadyError) Is(target error) bool {
	_, ok := target.(*DBNotReadyError)
	return ok
}

// Store defaults
const (
	DefaultSnapshotInterval  = 24 * time.Hour
	DefaultSnapshotRetention = 24 * time.Hour

	// DefaultSnapshotRetryInterval is the initial delay before retrying a
	// failed snapshot. It doubles on each consecutive failure, up to the
	// snapshot interval, so a transient failure does not leave the newest
	// snapshot a full interval older than intended.
	DefaultSnapshotRetryInterval = 1 * time.Minute

	DefaultRetention              = 24 * time.Hour
	DefaultRetentionCheckInterval = 1 * time.Hour

	// DefaultL0Retention is the default time that L0 files are kept around
	// after they have been compacted into L1 files.
	DefaultL0Retention = 5 * time.Minute
	// DefaultL0RetentionCheckInterval controls how frequently L0 retention is
	// enforced. This interval should be more frequent than the L1 compaction
	// interval so that VFS read replicas have time to observe new files.
	DefaultL0RetentionCheckInterval = 15 * time.Second

	// DefaultHeartbeatCheckInterval controls how frequently the heartbeat
	// monitor checks if heartbeat pings should be sent.
	DefaultHeartbeatCheckInterval = 15 * time.Second

	// DefaultDBInitTimeout is the maximum time to wait for a database to be
	// initialized (page size known) before logging a warning.
	DefaultDBInitTimeout = 30 * time.Second
)

// Store represents the top-level container for databases.
//
// It manages async background tasks like compactions so that the system
// is not overloaded by too many concurrent tasks.
type Store struct {
	mu     sync.Mutex
	dbs    []*DB
	levels CompactionLevels

	wg     sync.WaitGroup
	ctx    context.Context
	cancel func()
	done   <-chan struct{}

	// The frequency of snapshots.
	SnapshotInterval time.Duration
	// The duration of time that snapshots are kept before being deleted.
	SnapshotRetention time.Duration
	// The initial delay before retrying a failed snapshot (see
	// DefaultSnapshotRetryInterval).
	SnapshotRetryInterval time.Duration

	// SnapshotMode selects how periodic snapshots are produced (live db+wal read
	// vs. compaction of existing remote LTX files). Defaults to SnapshotModeLive.
	SnapshotMode SnapshotMode

	// The duration that L0 files are kept after being compacted into L1.
	L0Retention time.Duration
	// How often to check for expired L0 files.
	L0RetentionCheckInterval time.Duration

	// If true, compaction is run in the background according to compaction levels.
	CompactionMonitorEnabled bool

	// CompactionSerialize controls per-DB serialization of compactions/snapshots
	// in CompactDB. Defaults to SerializeAll.
	CompactionSerialize CompactionSerializeMode

	// If true, verify TXID consistency at destination level after each compaction.
	VerifyCompaction bool

	// RetentionEnabled controls whether Litestream actively deletes old files
	// during retention enforcement. When false, cloud provider lifecycle
	// policies handle retention instead. Local file cleanup still occurs.
	RetentionEnabled bool

	// Shutdown sync retry settings.
	ShutdownSyncTimeout  time.Duration
	ShutdownSyncInterval time.Duration

	// How often to check if heartbeat pings should be sent.
	HeartbeatCheckInterval time.Duration

	// Heartbeat client for health check pings. Sends pings only when
	// all databases have synced successfully within the heartbeat interval.
	Heartbeat *HeartbeatClient

	// heartbeatMonitorRunning tracks whether the heartbeat monitor goroutine is running.
	heartbeatMonitorRunning bool

	// How often to run validation checks. Zero disables periodic validation.
	ValidationInterval time.Duration

	Logger *slog.Logger
}

func NewStore(dbs []*DB, levels CompactionLevels) *Store {
	s := &Store{
		dbs:    dbs,
		levels: levels,

		SnapshotInterval:         DefaultSnapshotInterval,
		SnapshotRetention:        DefaultSnapshotRetention,
		SnapshotRetryInterval:    DefaultSnapshotRetryInterval,
		SnapshotMode:             SnapshotModeLive,
		L0Retention:              DefaultL0Retention,
		L0RetentionCheckInterval: DefaultL0RetentionCheckInterval,
		CompactionMonitorEnabled: true,
		CompactionSerialize:      SerializeAll,
		RetentionEnabled:         true,
		ShutdownSyncTimeout:      DefaultShutdownSyncTimeout,
		ShutdownSyncInterval:     DefaultShutdownSyncInterval,
		HeartbeatCheckInterval:   DefaultHeartbeatCheckInterval,
		Logger:                   slog.Default().With(LogKeySystem, LogSystemStore),
	}

	for _, db := range dbs {
		db.SetLogger(s.Logger.With(LogKeyDB, filepath.Base(db.Path())))
		db.L0Retention = s.L0Retention
		db.ShutdownSyncTimeout = s.ShutdownSyncTimeout
		db.ShutdownSyncInterval = s.ShutdownSyncInterval
		db.VerifyCompaction = s.VerifyCompaction
		db.RetentionEnabled = s.RetentionEnabled
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	return s
}

func (s *Store) Open(ctx context.Context) error {
	if err := s.levels.Validate(); err != nil {
		return err
	}

	initGroup, initCtx := errgroup.WithContext(ctx)
	initGroup.SetLimit(50)
	for _, db := range s.dbs {
		db := db
		initGroup.Go(func() error {
			select {
			case <-initCtx.Done():
				return initCtx.Err()
			default:
			}

			if db.Replica != nil && db.Replica.Client != nil {
				if err := db.Replica.Client.Init(initCtx); err != nil {
					return fmt.Errorf("initialize replica client for %q: %w", db.Path(), err)
				}
			}
			return nil
		})
	}
	if err := initGroup.Wait(); err != nil {
		return err
	}

	openGroup, openCtx := errgroup.WithContext(ctx)
	openGroup.SetLimit(50)
	for _, db := range s.dbs {
		db := db
		openGroup.Go(func() error {
			select {
			case <-openCtx.Done():
				return openCtx.Err()
			default:
			}
			return db.Open()
		})
	}
	if err := openGroup.Wait(); err != nil {
		return err
	}

	// Start monitors for compactions & snapshots.
	if s.CompactionMonitorEnabled {
		// Start compaction monitors for all levels except L0.
		for _, lvl := range s.levels {
			lvl := lvl
			if lvl.Level == 0 {
				continue
			}

			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.monitorCompactionLevel(s.ctx, lvl)
			}()
		}

		// Start snapshot monitor for snapshots.
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.monitorCompactionLevel(s.ctx, s.SnapshotLevel())
		}()
	}

	if s.L0Retention > 0 && s.L0RetentionCheckInterval > 0 {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.monitorL0Retention(s.ctx)
		}()
	}

	// Start heartbeat monitor if any database has heartbeat configured.
	s.startHeartbeatMonitorIfNeeded()

	// Start validation monitor if configured.
	if s.ValidationInterval > 0 {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.monitorValidation(s.ctx)
		}()
	}

	return nil
}

func (s *Store) Close(ctx context.Context) (err error) {
	// Stop the background monitors (snapshot, compaction, retention,
	// validation) before closing the databases they operate on. Closing
	// first left a monitor free to start a snapshot or compaction against a
	// database whose handles were being torn down underneath it.
	s.cancel()
	s.wg.Wait()

	s.mu.Lock()
	dbs := slices.Clone(s.dbs)
	s.mu.Unlock()

	for _, db := range dbs {
		if e := db.Close(ctx); e != nil {
			if errors.Is(e, ErrShutdownInterrupted) {
				if err == nil {
					err = e
				}
			} else if err == nil || errors.Is(err, ErrShutdownInterrupted) {
				err = e
			}
		}
	}

	return err
}

func (s *Store) DBs() []*DB {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.dbs)
}

// RegisterDB registers a new database with the store and starts monitoring it.
func (s *Store) RegisterDB(db *DB) error {
	if db == nil {
		return fmt.Errorf("db required")
	}

	// First check: see if database already exists
	s.mu.Lock()
	for _, existing := range s.dbs {
		if existing.Path() == db.Path() {
			s.mu.Unlock()
			return nil
		}
	}
	s.mu.Unlock()

	// Apply store-wide settings before opening the database.
	db.SetLogger(s.Logger.With(LogKeyDB, filepath.Base(db.Path())))
	db.L0Retention = s.L0Retention
	db.ShutdownSyncTimeout = s.ShutdownSyncTimeout
	db.ShutdownSyncInterval = s.ShutdownSyncInterval
	db.VerifyCompaction = s.VerifyCompaction
	db.RetentionEnabled = s.RetentionEnabled
	db.Done = s.done

	// Open the database without holding the lock to avoid blocking other operations.
	// The double-check pattern below handles the race condition.
	if err := db.Open(); err != nil {
		return fmt.Errorf("open db: %w", err)
	}

	// Second check: verify database wasn't added by another goroutine while we were opening.
	// If it was, close our instance and return without error.
	s.mu.Lock()

	for _, existing := range s.dbs {
		if existing.Path() == db.Path() {
			// Another goroutine added this database while we were opening.
			// Release lock before closing to avoid potential deadlock.
			s.mu.Unlock()
			if err := db.Close(context.Background()); err != nil {
				db.Logger.Error("close duplicate db", "path", db.Path(), "error", err)
			}
			return nil
		}
	}

	s.dbs = append(s.dbs, db)
	s.mu.Unlock()

	// Start heartbeat monitor if heartbeat is configured and monitor isn't running.
	s.startHeartbeatMonitorIfNeeded()

	return nil
}

// UnregisterDB stops monitoring the database at the provided path and closes it.
func (s *Store) UnregisterDB(ctx context.Context, path string) error {
	if path == "" {
		return fmt.Errorf("db path required")
	}

	s.mu.Lock()

	idx := -1
	var db *DB
	for i, existing := range s.dbs {
		if existing.Path() == path {
			idx = i
			db = existing
			break
		}
	}

	if db == nil {
		s.mu.Unlock()
		return nil
	}

	s.dbs = slices.Delete(s.dbs, idx, idx+1)
	s.mu.Unlock()

	if err := db.Close(ctx); err != nil {
		return fmt.Errorf("close db: %w", err)
	}

	return nil
}

// EnableDB starts replication for a registered database.
// The context is checked for cancellation before opening.
// Note: db.Open() itself does not support cancellation.
func (s *Store) EnableDB(ctx context.Context, path string) error {
	db := s.FindDB(path)
	if db == nil {
		return fmt.Errorf("database not found: %s", path)
	}

	if db.IsOpen() {
		return fmt.Errorf("database already enabled: %s", path)
	}

	// Check for cancellation before starting open
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("enable database: %w", err)
	}

	if err := db.Open(); err != nil {
		return fmt.Errorf("open database: %w", err)
	}

	return nil
}

// DisableDB stops replication for a database.
func (s *Store) DisableDB(ctx context.Context, path string) error {
	db := s.FindDB(path)
	if db == nil {
		return fmt.Errorf("database not found: %s", path)
	}

	if !db.IsOpen() {
		return fmt.Errorf("database already disabled: %s", path)
	}

	if err := db.Close(ctx); err != nil {
		return fmt.Errorf("close database: %w", err)
	}

	return nil
}

// SyncDBResult holds the result of a sync operation.
type SyncDBResult struct {
	TXID           uint64
	ReplicatedTXID uint64
	Changed        bool
}

// SyncDB forces an immediate sync for a database. If wait is true, blocks
// until both WAL-to-LTX and LTX-to-remote sync complete. If wait is false,
// only performs the WAL-to-LTX sync and lets the replica monitor handle upload.
// Lock waits are context-aware: the timeout is honored while waiting for
// the database sync executor and the replica sync lock.
func (s *Store) SyncDB(ctx context.Context, path string, wait bool) (SyncDBResult, error) {
	db := s.FindDB(path)
	if db == nil {
		return SyncDBResult{}, fmt.Errorf("%w: %s", ErrDatabaseNotFound, path)
	}

	if !db.IsOpen() {
		return SyncDBResult{}, fmt.Errorf("%w: %s", ErrDatabaseNotOpen, path)
	}

	_, beforeTXID, err := db.MaxLTX()
	if err != nil {
		return SyncDBResult{}, fmt.Errorf("read position before sync: %w", err)
	}

	if wait {
		if err := db.SyncAndWait(ctx); err != nil {
			return SyncDBResult{}, fmt.Errorf("sync database: %w", err)
		}
	} else {
		if err := db.requestOrSync(ctx); err != nil {
			return SyncDBResult{}, fmt.Errorf("sync database: %w", err)
		}
	}

	_, afterTXID, err := db.MaxLTX()
	if err != nil {
		return SyncDBResult{}, fmt.Errorf("read position after sync: %w", err)
	}

	var replicatedTXID uint64
	if db.Replica != nil {
		replicatedTXID = uint64(db.Replica.Pos().TXID)
	}

	return SyncDBResult{
		TXID:           uint64(afterTXID),
		ReplicatedTXID: replicatedTXID,
		Changed:        afterTXID > beforeTXID,
	}, nil
}

// FindDB returns the database with the given path.
func (s *Store) FindDB(path string) *DB {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, db := range s.dbs {
		if db.Path() == path {
			return db
		}
	}
	return nil
}

// SetL0Retention updates the retention window for L0 files and propagates it to
// all managed databases.
func (s *Store) SetL0Retention(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.L0Retention = d
	for _, db := range s.dbs {
		db.L0Retention = d
	}
}

// SetDone sets the done channel used for interrupt handling during shutdown
// and propagates it to all managed databases.
func (s *Store) SetDone(done <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.done = done
	for _, db := range s.dbs {
		db.Done = done
	}
}

// SetShutdownSyncTimeout updates the shutdown sync timeout and propagates it to
// all managed databases.
func (s *Store) SetShutdownSyncTimeout(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ShutdownSyncTimeout = d
	for _, db := range s.dbs {
		db.ShutdownSyncTimeout = d
	}
}

// SetShutdownSyncInterval updates the shutdown sync interval and propagates it to
// all managed databases.
func (s *Store) SetShutdownSyncInterval(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ShutdownSyncInterval = d
	for _, db := range s.dbs {
		db.ShutdownSyncInterval = d
	}
}

// SetVerifyCompaction updates the verify compaction flag and propagates it to
// all managed databases.
func (s *Store) SetVerifyCompaction(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.VerifyCompaction = v
	for _, db := range s.dbs {
		db.VerifyCompaction = v
		db.compactor.VerifyCompaction = v
	}
}

func (s *Store) SetRetentionEnabled(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.RetentionEnabled = v
	for _, db := range s.dbs {
		db.RetentionEnabled = v
		db.compactor.RetentionEnabled = v
	}
}

// SnapshotLevel returns a pseudo compaction level based on snapshot settings.
func (s *Store) SnapshotLevel() *CompactionLevel {
	return &CompactionLevel{
		Level:    SnapshotLevel,
		Interval: s.SnapshotInterval,
	}
}

func (s *Store) monitorCompactionLevel(ctx context.Context, lvl *CompactionLevel) {
	s.Logger.Info("starting compaction monitor", "level", lvl.Level, "interval", lvl.Interval)

	retryDeadline := time.Time{}
	var snapshotRetryDelay time.Duration // zero while the last snapshot tick succeeded
	timer := time.NewTimer(time.Nanosecond)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			// proceed
		}

		now := time.Now()
		nextDelay := time.Until(lvl.NextCompactionAt(now))

		var notReadyDBs []string
		var snapshotRetry bool

		for _, db := range s.DBs() {
			if !db.IsOpen() {
				continue // skip disabled DBs
			}
			_, err := s.CompactDB(ctx, db, lvl)
			switch {
			case errors.Is(err, ErrNoCompaction), errors.Is(err, ErrCompactionTooEarly):
				db.Logger.Debug("no compaction", "level", lvl.Level, "path", db.Path())
			case errors.Is(err, ErrCompactionInProgress):
				db.Logger.Debug("compaction already in progress for db, skipping", "level", lvl.Level, "path", db.Path())
				snapshotRetry = true
			case errors.Is(err, ErrSnapshotPending):
				db.Logger.Info("snapshot waiting for replication", "path", db.Path())
				snapshotRetry = true
			case errors.Is(err, ErrDBNotReady):
				db.Logger.Debug("db not ready, skipping", "level", lvl.Level, "path", db.Path(), "error", err)
				notReadyDBs = append(notReadyDBs, db.Path())
			case err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded):
				db.Logger.Error("compaction failed", "level", lvl.Level, "error", err)
				snapshotRetry = true
			}

			if lvl.Level == SnapshotLevel {
				if err := s.EnforceSnapshotRetention(ctx, db); err != nil &&
					!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					db.Logger.Error("retention enforcement failed", "error", err)
				}
			}
		}

		timedOut := !retryDeadline.IsZero() && now.After(retryDeadline)
		if len(notReadyDBs) > 0 && !timedOut {
			if retryDeadline.IsZero() {
				retryDeadline = now.Add(DefaultDBInitTimeout)
			}
			nextDelay = time.Second
			s.Logger.Debug("scheduling retry for unready dbs", "level", lvl.Level)
		} else {
			if timedOut {
				s.Logger.Warn("timeout waiting for db initialization",
					"level", lvl.Level,
					"dbs", notReadyDBs,
					"timeout", DefaultDBInitTimeout,
					"hint", "database may have corrupted local state or blocked transactions; try removing -litestream directory and restarting")
			}
			retryDeadline = time.Time{}
		}

		// A snapshot that failed (or was skipped because another compaction
		// held the DB) is retried with backoff rather than waiting a full
		// interval. The retry gets past CompactDB's ErrCompactionTooEarly
		// gate because the newest snapshot still predates this interval.
		if lvl.Level == SnapshotLevel {
			if snapshotRetry {
				snapshotRetryDelay = nextSnapshotRetryDelay(snapshotRetryDelay, s.SnapshotRetryInterval, lvl.Interval)
				if snapshotRetryDelay < nextDelay {
					nextDelay = snapshotRetryDelay
					s.Logger.Info("scheduling snapshot retry", "delay", snapshotRetryDelay)
				}
			} else {
				snapshotRetryDelay = 0
			}
		}

		if nextDelay < 0 {
			nextDelay = 0
		}
		timer.Reset(nextDelay)
	}
}

// nextSnapshotRetryDelay returns the delay before the next snapshot retry:
// initial on the first failure, doubling on each consecutive one, capped at
// max. A non-positive initial disables the early retry by returning max.
func nextSnapshotRetryDelay(prev, initial, max time.Duration) time.Duration {
	if initial <= 0 {
		return max
	}
	next := initial
	if prev > 0 {
		next = prev * 2
	}
	if max > 0 && next > max {
		next = max
	}
	return next
}

func (s *Store) monitorL0Retention(ctx context.Context) {
	s.Logger.Info("starting L0 retention monitor", "interval", s.L0RetentionCheckInterval, "retention", s.L0Retention)

	ticker := time.NewTicker(s.L0RetentionCheckInterval)
	defer ticker.Stop()

LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		case <-ticker.C:
		}

		for _, db := range s.DBs() {
			if !db.IsOpen() {
				continue // skip disabled DBs
			}
			if err := db.EnforceL0RetentionByTime(ctx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				db.Logger.Error("l0 retention enforcement failed", "path", db.Path(), "error", err)
			}
		}
	}
}

// startHeartbeatMonitorIfNeeded starts the heartbeat monitor goroutine if:
// - HeartbeatCheckInterval is configured
// - Heartbeat is configured on the Store
// - The monitor is not already running
func (s *Store) startHeartbeatMonitorIfNeeded() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.heartbeatMonitorRunning {
		return
	}
	if s.HeartbeatCheckInterval <= 0 {
		return
	}
	if !s.hasHeartbeatConfigLocked() {
		return
	}

	s.heartbeatMonitorRunning = true
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.monitorHeartbeats(s.ctx)
	}()
}

// hasHeartbeatConfigLocked returns true if heartbeat is configured on the Store.
// Must be called with s.mu held.
func (s *Store) hasHeartbeatConfigLocked() bool {
	return s.Heartbeat != nil && s.Heartbeat.URL != ""
}

// monitorHeartbeats periodically checks if heartbeat pings should be sent.
// Heartbeat pings are only sent when ALL databases have synced successfully
// within the heartbeat interval.
func (s *Store) monitorHeartbeats(ctx context.Context) {
	s.Logger.Info("starting heartbeat monitor", "interval", s.HeartbeatCheckInterval)

	ticker := time.NewTicker(s.HeartbeatCheckInterval)
	defer ticker.Stop()

LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		case <-ticker.C:
		}

		s.sendHeartbeatIfNeeded(ctx)
	}
}

// sendHeartbeatIfNeeded sends a heartbeat ping if:
// - Heartbeat is configured on the Store
// - Enough time has passed since the last ping attempt
// - ALL databases have synced successfully within the heartbeat interval
func (s *Store) sendHeartbeatIfNeeded(ctx context.Context) {
	hb := s.Heartbeat
	if hb == nil || hb.URL == "" {
		return
	}

	if !hb.ShouldPing() {
		return
	}

	// Check if all databases are healthy (synced within the heartbeat interval).
	// A database is healthy if it synced within the heartbeat interval.
	healthySince := time.Now().Add(-hb.Interval)
	if !s.allDatabasesHealthy(healthySince) {
		return
	}

	// Record ping attempt time before making the request to ensure we respect
	// the configured interval even if the ping fails. This prevents rapid
	// retries that could overwhelm the endpoint.
	hb.RecordPing()

	if err := hb.Ping(ctx); err != nil {
		s.Logger.Error("heartbeat ping failed", "url", hb.URL, "error", err)
		return
	}

	s.Logger.Debug("heartbeat ping sent", "url", hb.URL)
}

// allDatabasesHealthy returns true if all databases have synced successfully
// since the given time. Returns false if there are no databases or no enabled databases.
func (s *Store) allDatabasesHealthy(since time.Time) bool {
	dbs := s.DBs()
	if len(dbs) == 0 {
		return false
	}

	enabledCount := 0
	for _, db := range dbs {
		if !db.IsOpen() {
			continue // skip disabled DBs
		}
		enabledCount++
		lastSync := db.LastSuccessfulSyncAt()
		if lastSync.IsZero() || lastSync.Before(since) {
			return false
		}
	}
	return enabledCount > 0
}

// CompactDB performs a compaction or snapshot for a given database on a single destination level.
// This function will only proceed if a compaction has not occurred before the last compaction time.
func (s *Store) CompactDB(ctx context.Context, db *DB, lvl *CompactionLevel) (*ltx.FileInfo, error) {
	dstLevel := lvl.Level

	// Serialize compaction/snapshot per database. The snapshot level and each
	// compaction level run in independent goroutines (see Store.Open), so
	// without this guard a snapshot (L9) and a lower-level compaction can run
	// concurrently for the same DB. Each retains a database-sized in-memory page
	// index, and on a large database the overlap can exhaust the memory limit
	// and trigger an OOM kill. If another compaction is already running for this
	// DB, skip this tick; the monitor retries on its next interval.
	//
	// The set of operations that participate is controlled by CompactionSerialize:
	//   - SerializeAll: every level takes the lock (historical behavior).
	//   - SerializeHeavy: only the snapshot level (L9, the sole O(db-size)
	//     operation) takes the lock, so incremental compactions are not starved.
	//   - SerializeOff: no level takes the lock.
	serialize := s.CompactionSerialize == SerializeAll ||
		(s.CompactionSerialize == SerializeHeavy && dstLevel == SnapshotLevel)
	if serialize {
		if !db.compactionMu.TryLock() {
			return nil, ErrCompactionInProgress
		}
		defer db.compactionMu.Unlock()
	}

	// Skip if database is not yet initialized (page size unknown).
	if db.PageSize() == 0 {
		return nil, &DBNotReadyError{Reason: "page size not initialized"}
	}

	// Ensure we are not re-compacting before the most recent compaction time.
	prevCompactionAt := lvl.PrevCompactionAt(time.Now())
	dstInfo, err := db.MaxLTXFileInfo(ctx, dstLevel)
	if err != nil {
		return nil, fmt.Errorf("fetch dst level info: %w", err)
	} else if dstInfo.CreatedAt.After(prevCompactionAt) {
		return nil, ErrCompactionTooEarly
	}

	// Shortcut if this is a snapshot since we are not pulling from a previous level.
	if dstLevel == SnapshotLevel {
		pos, err := db.Pos()
		if err != nil {
			return nil, fmt.Errorf("fetch db position: %w", err)
		}
		// The base (TXID 1) is the sync path's responsibility: it writes the
		// initial full-DB image and the replica uploads it to the snapshot
		// level. The monitor only produces periodic snapshots once the DB has
		// advanced past the base (pos > 1). Standing down at pos <= 1 avoids
		// a concurrent second full-DB read that would otherwise race the sync
		// path's base capture.
		if dstInfo.MaxTXID == 0 && pos.TXID <= 1 {
			return nil, ErrNoCompaction
		}
		if dstInfo.MaxTXID != 0 && dstInfo.MaxTXID >= pos.TXID {
			if s.SnapshotMode != SnapshotModeCompact {
				return nil, ErrNoCompaction
			}
			// The database is idle: nothing has been written since the newest
			// snapshot, which the ErrCompactionTooEarly gate above shows is at
			// least an interval old. Compact mode promises that a restore needs
			// only files younger than about one interval (so object-age
			// lifecycle policies can clean up the replica), so refresh the
			// snapshot anyway. Writing a real transaction gives the restore plan
			// an increment to fold in, so the merge mints [1, k+1] under a new
			// name instead of overwriting [1, k] in place, which could splice
			// bytes under a concurrent reader that reopens mid-stream. The
			// database's own sync picks the write up and the replica uploads
			// it; the monitor's retry then takes the snapshot.
			if err := db.bumpLitestreamSeq(ctx); err != nil {
				return nil, fmt.Errorf("bump seq to refresh idle snapshot: %w", err)
			}
			db.Logger.InfoContext(ctx, "refreshing snapshot of idle database", "txid", dstInfo.MaxTXID.String())
			return nil, ErrSnapshotPending
		}

		var info *ltx.FileInfo
		switch s.SnapshotMode {
		case SnapshotModeCompact:
			info, err = db.SnapshotCompact(ctx)
			if errors.Is(err, ErrNoCompaction) {
				// The database is past the newest snapshot, but the replica
				// holds no increments yet: retry once they are uploaded.
				return nil, ErrSnapshotPending
			}
		default:
			info, err = db.Snapshot(ctx)
		}
		if err != nil {
			return info, err
		}
		db.Logger.InfoContext(ctx, "snapshot complete", "mode", s.SnapshotMode.String(), "txid", info.MaxTXID.String(), "size", info.Size)
		return info, nil
	}

	// Fetch latest LTX files for both the source & destination so we can see if we need to make progress.
	srcLevel := s.levels.PrevLevel(dstLevel)
	srcInfo, err := db.MaxLTXFileInfo(ctx, srcLevel)
	if err != nil {
		return nil, fmt.Errorf("fetch src level info: %w", err)
	}

	// Skip if there are no new files to compact.
	if srcInfo.MaxTXID <= dstInfo.MinTXID {
		return nil, ErrNoCompaction
	}

	info, err := db.Compact(ctx, dstLevel)
	if err != nil {
		return info, err
	}

	db.Logger.InfoContext(ctx, "compaction complete",
		"level", dstLevel,
		slog.Group("txid",
			"min", info.MinTXID.String(),
			"max", info.MaxTXID.String(),
		),
		"size", info.Size,
	)

	return info, nil
}

// EnforceSnapshotRetention removes old snapshots by timestamp and then
// cleans up all lower levels based on minimum snapshot TXID.
func (s *Store) EnforceSnapshotRetention(ctx context.Context, db *DB) error {
	// Enforce retention for the snapshot level.
	minSnapshotTXID, err := db.EnforceSnapshotRetention(ctx, time.Now().Add(-s.SnapshotRetention))
	if err != nil {
		return fmt.Errorf("enforce snapshot retention: %w", err)
	}

	// We should also enforce retention for L0 on the same schedule as L1.
	for _, lvl := range s.levels {
		// Skip L0 since it is enforced on a more frequent basis.
		if lvl.Level == 0 {
			continue
		}

		if err := db.EnforceRetentionByTXID(ctx, lvl.Level, minSnapshotTXID); err != nil {
			return fmt.Errorf("enforce L%d retention: %w", lvl.Level, err)
		}
	}

	return nil
}

// ValidationResult holds the result of validating a replica's LTX files.
type ValidationResult struct {
	Valid  bool              // true if no errors found
	Errors []ValidationError // all errors found
}

// Validate checks LTX file consistency across all databases and levels.
// SnapshotLevel (9) is excluded since snapshots are not contiguous.
func (s *Store) Validate(ctx context.Context) (*ValidationResult, error) {
	result := &ValidationResult{Valid: true}

	s.mu.Lock()
	dbs := s.dbs
	levels := s.levels
	s.mu.Unlock()

	for _, db := range dbs {
		if db.Replica == nil {
			continue
		}

		for _, lvl := range levels {
			errs, err := db.Replica.ValidateLevel(ctx, lvl.Level)
			if err != nil {
				return nil, fmt.Errorf("validate level %d for %s: %w", lvl.Level, db.Path(), err)
			}
			if len(errs) > 0 {
				result.Valid = false
				result.Errors = append(result.Errors, errs...)
			}
		}
	}

	return result, nil
}

// monitorValidation periodically runs validation checks on all databases.
func (s *Store) monitorValidation(ctx context.Context) {
	s.Logger.Info("starting validation monitor", "interval", s.ValidationInterval)

	ticker := time.NewTicker(s.ValidationInterval)
	defer ticker.Stop()

LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		case <-ticker.C:
		}

		result, err := s.Validate(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			s.Logger.Error("validation check failed", "error", err)
			continue
		}

		if !result.Valid {
			for _, verr := range result.Errors {
				s.Logger.Warn("validation error detected",
					"level", verr.Level,
					"type", verr.Type,
					"message", verr.Message,
				)
			}
		}
	}
}
