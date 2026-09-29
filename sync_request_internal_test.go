package litestream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

// openMonitoredDB opens a DB whose monitor only runs sync passes when
// requested (the monitor interval is effectively infinite) and whose replica
// monitor uploads promptly after each pass. Each setup func runs before Open,
// i.e. before the monitor goroutines start.
func openMonitoredDB(t *testing.T, client ReplicaClient, setup ...func(*DB)) (*DB, *sql.DB) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "db")

	db := NewDB(dbPath)
	db.MonitorInterval = time.Hour
	db.ShutdownSyncTimeout = 0
	r := NewReplicaWithClient(db, client)
	r.SyncInterval = 10 * time.Millisecond
	db.Replica = r
	for _, fn := range setup {
		fn(db)
	}
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`PRAGMA journal_mode = wal;`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY);`); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = sqldb.Close()
		if err := db.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return db, sqldb
}

func syncPasses(db *DB) (started, done uint64, err error) {
	db.passes.Lock()
	defer db.passes.Unlock()
	return db.passes.started, db.passes.done, db.passes.err
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func mustInsert(t *testing.T, sqldb *sql.DB) {
	t.Helper()
	if _, err := sqldb.Exec(`INSERT INTO t DEFAULT VALUES;`); err != nil {
		t.Fatal(err)
	}
}

func TestDB_RequestSync_SealsCommittedWrites(t *testing.T) {
	db, sqldb := openMonitoredDB(t, &testReplicaClient{dir: t.TempDir()})

	if err := db.requestSync(t.Context()); err != nil {
		t.Fatal(err)
	}
	before, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}

	mustInsert(t, sqldb)
	if err := db.requestSync(t.Context()); err != nil {
		t.Fatal(err)
	}
	after, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	if after.TXID <= before.TXID {
		t.Fatalf("txid=%s after insert, want > %s", after.TXID, before.TXID)
	}
}

// Requests made while a pass is in progress must coalesce into a single
// follow-up pass, and abandoned waits must not queue additional syncs.
func TestDB_RequestSync_CoalescesWhileSyncInProgress(t *testing.T) {
	db, sqldb := openMonitoredDB(t, &testReplicaClient{dir: t.TempDir()})

	// Stall the next pass on the executor lock, as a long sync would.
	mustAcquireSemaphore(db.execSem)
	first := make(chan error, 1)
	go func() { first <- db.requestSync(context.Background()) }()
	waitFor(t, "first pass to start", func() bool {
		started, _, _ := syncPasses(db)
		return started == 1
	})

	mustInsert(t, sqldb)
	for range 10 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		err := db.requestSync(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err=%v, want deadline exceeded", err)
		}
	}
	if started, _, _ := syncPasses(db); started != 1 {
		t.Fatalf("started=%d while first pass is in progress, want 1", started)
	}

	db.execSem.Release(1)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	waitFor(t, "follow-up pass", func() bool {
		_, done, _ := syncPasses(db)
		return done == 2
	})

	// No further passes are pending.
	time.Sleep(50 * time.Millisecond)
	if started, done, err := syncPasses(db); started != 2 || done != 2 || err != nil {
		t.Fatalf("started=%d done=%d err=%v, want exactly 2 successful passes", started, done, err)
	}
}

// While sync requests keep arriving they drive all syncs: each requested pass
// resets the monitor tick, which only fires once requests stop for a full
// interval. Otherwise the two schedules would each seal their own LTX files.
func TestDB_RequestSync_PostponesMonitorTick(t *testing.T) {
	const interval = 200 * time.Millisecond
	db, _ := openMonitoredDB(t, &testReplicaClient{dir: t.TempDir()}, func(db *DB) {
		db.MonitorInterval = interval
	})

	// The ticker starts at Open, so count passes from the first request on.
	if err := db.requestSync(t.Context()); err != nil {
		t.Fatal(err)
	}
	baseline, _, _ := syncPasses(db)

	// Five intervals' worth of requests, each well within an interval of the last.
	const requests = 20
	for range requests {
		time.Sleep(interval / 4)
		if err := db.requestSync(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if started, _, _ := syncPasses(db); started != baseline+requests {
		t.Fatalf("started=%d, want %d: the monitor tick fired while requests were arriving",
			started, baseline+requests)
	}

	waitFor(t, "monitor tick after requests stop", func() bool {
		started, _, _ := syncPasses(db)
		return started > baseline+requests
	})
}

// A request must only be satisfied by a pass that started after the request,
// because a pass that was already in progress may have read the WAL before the
// requester's latest commit.
func TestDB_RequestSync_WaitsForPassStartedAfterRequest(t *testing.T) {
	db, sqldb := openMonitoredDB(t, &testReplicaClient{dir: t.TempDir()})

	mustAcquireSemaphore(db.execSem)
	first := make(chan error, 1)
	go func() { first <- db.requestSync(context.Background()) }()
	waitFor(t, "first pass to start", func() bool {
		started, _, _ := syncPasses(db)
		return started == 1
	})

	mustInsert(t, sqldb)
	second := make(chan uint64, 1)
	go func() {
		if err := db.requestSync(context.Background()); err != nil {
			t.Error(err)
		}
		_, done, _ := syncPasses(db)
		second <- done
	}()

	db.execSem.Release(1)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if done := <-second; done < 2 {
		t.Fatalf("second request returned after pass %d, want a pass started after it (>= 2)", done)
	}
}

// Canceling a request abandons the wait but must not interrupt the sync.
func TestDB_RequestSync_CancelDoesNotInterruptSync(t *testing.T) {
	db, sqldb := openMonitoredDB(t, &testReplicaClient{dir: t.TempDir()})

	mustAcquireSemaphore(db.execSem)
	mustInsert(t, sqldb)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- db.requestSync(ctx) }()
	waitFor(t, "pass to start", func() bool {
		started, _, _ := syncPasses(db)
		return started == 1
	})
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want canceled", err)
	}

	db.execSem.Release(1)
	waitFor(t, "pass to complete", func() bool {
		_, done, _ := syncPasses(db)
		return done >= 1
	})
	if _, _, err := syncPasses(db); err != nil {
		t.Fatalf("pass err=%v, want the abandoned sync to complete", err)
	}
}

// A failed pass must fail the requests waiting on it: the requester's commits
// may not have been sealed, so reporting success would claim durability that
// doesn't exist.
func TestDB_SyncAndWait_ReturnsFailedPassError(t *testing.T) {
	errInjected := errors.New("injected ltx open failure")
	var fail atomic.Bool
	db, sqldb := openMonitoredDB(t, &testReplicaClient{dir: t.TempDir()}, func(db *DB) {
		db.openLTXFile = func(name string, flag int, perm os.FileMode) (ltxStagingFile, error) {
			if fail.Load() {
				return nil, errInjected
			}
			return defaultOpenLTXFile(name, flag, perm)
		}
	})

	fail.Store(true)
	t.Cleanup(func() { fail.Store(false) }) // runs before db.Close's final sync
	mustInsert(t, sqldb)
	if err := db.SyncAndWait(t.Context()); !errors.Is(err, errInjected) {
		t.Fatalf("err=%v, want the failed pass's error", err)
	}
	if _, done, err := syncPasses(db); done != 1 || !errors.Is(err, errInjected) {
		t.Fatalf("done=%d err=%v, want exactly 1 failed pass", done, err)
	}
	if rpos := db.Replica.Pos(); !rpos.IsZero() {
		t.Fatalf("replica txid=%s, want nothing uploaded", rpos.TXID)
	}
}

// shortSocketPath returns a Unix socket path short enough for macOS's
// 104-byte limit, which t.TempDir() paths can exceed.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ls")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

// goroutinesIn returns the number of goroutines whose stack includes fn.
func goroutinesIn(fn string) int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	var count int
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, fn) {
			count++
		}
	}
	return count
}

// A /sync request whose client disconnects must release its handler goroutine
// immediately, even without a timeout, instead of staying parked until the pass
// it is waiting on completes. Parked handlers used to pile up behind long
// executor holds (e.g. a base snapshot of a large database).
func TestServer_HandleSync_ClientDisconnectReleasesHandler(t *testing.T) {
	const handler = "litestream.(*Server).handleSync"

	db, sqldb := openMonitoredDB(t, &testReplicaClient{dir: t.TempDir()})
	mustInsert(t, sqldb)

	s := NewServer(&Store{dbs: []*DB{db}})
	s.SocketPath = shortSocketPath(t)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Stall the pass the request waits on, as a long sync would.
	mustAcquireSemaphore(db.execSem)
	defer db.execSem.Release(1)

	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", s.SocketPath)
		},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/sync",
		strings.NewReader(fmt.Sprintf(`{"path": %q}`, db.Path())))
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
		errCh <- err
	}()

	waitFor(t, "handler to wait on a pass", func() bool {
		started, _, _ := syncPasses(db)
		return started == 1 && goroutinesIn(handler) == 1
	})

	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("client err=%v, want canceled", err)
	}
	waitFor(t, "handler to return after the client disconnected", func() bool {
		return goroutinesIn(handler) == 0
	})
	if _, done, _ := syncPasses(db); done != 0 {
		t.Fatalf("done=%d, want the handler released while the pass is still stalled", done)
	}
}

// uploadBlockingClient blocks uploads until released and records whether the
// upload's context was canceled.
type uploadBlockingClient struct {
	*testReplicaClient
	release  chan struct{}
	mu       sync.Mutex
	blocked  int
	canceled bool
}

func (c *uploadBlockingClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	c.mu.Lock()
	c.blocked++
	c.mu.Unlock()
	select {
	case <-c.release:
	case <-ctx.Done():
		c.mu.Lock()
		c.canceled = true
		c.mu.Unlock()
		return nil, ctx.Err()
	}
	return c.testReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
}

// SyncAndWait must wait for the replica monitor's upload rather than uploading
// on the caller's context, so a timed-out wait cannot abort the upload.
func TestDB_SyncAndWait_TimeoutDoesNotCancelUpload(t *testing.T) {
	client := &uploadBlockingClient{
		testReplicaClient: &testReplicaClient{dir: t.TempDir()},
		release:           make(chan struct{}),
	}
	db, sqldb := openMonitoredDB(t, client)
	mustInsert(t, sqldb)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := db.SyncAndWait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want deadline exceeded while upload is blocked", err)
	}
	client.mu.Lock()
	blocked := client.blocked
	client.mu.Unlock()
	if blocked == 0 {
		t.Fatal("expected the replica monitor to be uploading")
	}

	close(client.release)
	if err := db.SyncAndWait(t.Context()); err != nil {
		t.Fatal(err)
	}
	dpos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	if rpos := db.Replica.Pos(); rpos.TXID < dpos.TXID {
		t.Fatalf("replica txid=%s, want >= %s", rpos.TXID, dpos.TXID)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.canceled {
		t.Fatal("upload was canceled by the abandoned wait")
	}
}
