package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// rangeServer is a minimal S3 stand-in that answers ranged GETs for a single
// object and records every range it was asked for.
type rangeServer struct {
	data []byte

	// delay is called before each response body is written.
	delay func(start int64) time.Duration

	// truncateOnce aborts the first response for a given start offset after
	// writing half the body, simulating a connection dropped mid-part.
	truncateOnce map[int64]bool

	// maxBody caps every response body to this many bytes, so a part advances
	// only a little per connection. Zero means no cap.
	maxBody int

	// emptyBody answers a given start offset with a well-formed but empty 206,
	// so the part makes no progress and no transport error occurs. That isolates
	// the reader's own retry budget from the SDK's transport retryer, which
	// would otherwise retry an aborted connection ten times with backoff.
	emptyBody map[int64]bool

	mu     sync.Mutex
	ranges []int64 // start offset of every request, in arrival order
	gets   int
}

func newRangeServer(t *testing.T, size int) (*rangeServer, *httptest.Server) {
	t.Helper()

	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	rs := &rangeServer{
		data:         data,
		truncateOnce: make(map[int64]bool),
		emptyBody:    make(map[int64]bool),
	}
	server := httptest.NewServer(http.HandlerFunc(rs.serve))
	t.Cleanup(server.Close)
	return rs, server
}

func (rs *rangeServer) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Count every GET the server receives, including ones it answers with 416:
	// a request that reports the end of the object is still a request.
	rs.mu.Lock()
	rs.gets++
	rs.mu.Unlock()

	total := int64(len(rs.data))
	start, end, err := parseRangeHeader(r.Header.Get("Range"), total)
	if err != nil {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	rs.mu.Lock()
	rs.ranges = append(rs.ranges, start)
	truncate := rs.truncateOnce[start]
	if truncate {
		rs.truncateOnce[start] = false
	}
	empty := rs.emptyBody[start]
	rs.mu.Unlock()

	if rs.delay != nil {
		time.Sleep(rs.delay(start))
	}

	body := rs.data[start : end+1]
	if empty {
		body = nil
	}
	if rs.maxBody > 0 && len(body) > rs.maxBody {
		body = body[:rs.maxBody]
		end = start + int64(rs.maxBody) - 1
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusPartialContent)

	if truncate {
		// Write a short body and kill the connection so the client sees the
		// part end prematurely.
		_, _ = w.Write(body[:len(body)/2])
		panic(http.ErrAbortHandler)
	}
	_, _ = w.Write(body)
}

func parseRangeHeader(v string, total int64) (start, end int64, err error) {
	spec, ok := strings.CutPrefix(v, "bytes=")
	if !ok {
		return 0, total - 1, nil
	}
	lo, hi, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, fmt.Errorf("malformed range %q", v)
	}
	if start, err = strconv.ParseInt(lo, 10, 64); err != nil {
		return 0, 0, err
	}
	if start >= total {
		return 0, 0, fmt.Errorf("range %q past end of object", v)
	}
	end = total - 1
	if hi != "" {
		if end, err = strconv.ParseInt(hi, 10, 64); err != nil {
			return 0, 0, err
		}
		if end > total-1 {
			end = total - 1
		}
	}
	return start, end, nil
}

func (rs *rangeServer) stats() (gets int, starts []int64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.gets, append([]int64(nil), rs.ranges...)
}

// newMultipartTestClient returns a client wired to server with an isolated
// chunk pool so tests never touch the process-wide one.
func newMultipartTestClient(t *testing.T, server *httptest.Server, poolSize int, partSize int64) (*ReplicaClient, *chunkPool) {
	t.Helper()

	client := newTestReplicaClient(t, server)
	client.DownloadPartSize = partSize
	client.DownloadConcurrency = poolSize
	client.pool = newChunkPool(poolSize, partSize)
	return client, client.pool
}

func poolStats(p *chunkPool) (readers, surplus, free int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.readers, p.surplus, len(p.free)
}

// readWithChunks reads rc in randomly sized pieces to exercise partial reads
// across chunk boundaries.
func readWithChunks(t *testing.T, rc io.Reader, max int) []byte {
	t.Helper()

	var out []byte
	buf := make([]byte, max)
	for i := 0; ; i++ {
		n := 1 + (i*7+3)%max // varying, deterministic read sizes
		c, err := rc.Read(buf[:n])
		out = append(out, buf[:c]...)
		if err == io.EOF {
			return out
		} else if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
}

// TestOpenLTXFile_MultipartRoundTrip verifies that a parallel download
// reassembles the object byte-for-byte across a matrix of sizes and pool
// configurations, with the server answering parts at varying speeds.
func TestOpenLTXFile_MultipartRoundTrip(t *testing.T) {
	for _, tt := range []struct {
		size     int
		partSize int64
		poolSize int
	}{
		{size: 1234 * 1023, partSize: 1000, poolSize: 2},
		{size: 1234 * 1023, partSize: 1000, poolSize: 8},
		{size: 1234 * 1023, partSize: 4096, poolSize: 46},
		{size: 8193, partSize: 4096, poolSize: 4},  // exact-ish boundary
		{size: 8192, partSize: 4096, poolSize: 4},  // exactly two parts
		{size: 4097, partSize: 4096, poolSize: 4},  // one byte past a part
		{size: 100000, partSize: 512, poolSize: 6}, // many small parts
	} {
		t.Run(fmt.Sprintf("size=%d/part=%d/pool=%d", tt.size, tt.partSize, tt.poolSize), func(t *testing.T) {
			rs, server := newRangeServer(t, tt.size)
			// Later parts answer faster than earlier ones, so completions
			// arrive out of order and must be resequenced.
			rs.delay = func(start int64) time.Duration {
				return time.Duration(10-int64(start)%10) * time.Millisecond
			}

			client, pool := newMultipartTestClient(t, server, tt.poolSize, tt.partSize)

			rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, int64(tt.size))
			if err != nil {
				t.Fatal(err)
			}

			got := readWithChunks(t, rc, 7919)
			if err := rc.Close(); err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(got, rs.data) {
				t.Fatalf("downloaded %d bytes, want %d (equal=%v)", len(got), len(rs.data), bytes.Equal(got, rs.data))
			}
			// Guard against the plain single-GET path passing this trivially.
			if gets, _ := rs.stats(); gets < 2 {
				t.Fatalf("GET count = %d; the parallel path was not taken", gets)
			}

			readers, surplus, free := poolStats(pool)
			if readers != 0 || surplus != 0 {
				t.Fatalf("pool not drained: readers=%d surplus=%d", readers, surplus)
			}
			if free > tt.poolSize {
				t.Fatalf("pool holds %d buffers, size is %d", free, tt.poolSize)
			}
		})
	}
}

// TestOpenLTXFile_SmallObjectSingleGet verifies an object known to fit in one
// part costs a single GET and never touches the pool.
func TestOpenLTXFile_SmallObjectSingleGet(t *testing.T) {
	const partSize = 4096

	for _, size := range []int{1, 100, partSize - 1, partSize} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			rs, server := newRangeServer(t, size)
			client, pool := newMultipartTestClient(t, server, 8, partSize)

			rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, int64(size))
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatal(err)
			}
			if err := rc.Close(); err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(got, rs.data) {
				t.Fatalf("got %d bytes, want %d", len(got), len(rs.data))
			}
			if gets, _ := rs.stats(); gets != 1 {
				t.Fatalf("GET count = %d, want 1", gets)
			}
			if readers, _, free := poolStats(pool); readers != 0 || free != 0 {
				t.Fatalf("pool touched for small object: readers=%d free=%d", readers, free)
			}
		})
	}
}

// TestOpenLTXFile_MultipartDisabled verifies download-concurrency=0 keeps the
// previous single unbounded GET behavior.
func TestOpenLTXFile_MultipartDisabled(t *testing.T) {
	rs, server := newRangeServer(t, 100000)
	client := newTestReplicaClient(t, server)
	client.DownloadConcurrency = 0

	rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()

	if !bytes.Equal(got, rs.data) {
		t.Fatal("data mismatch")
	}
	gets, starts := rs.stats()
	if gets != 1 {
		t.Fatalf("GET count = %d, want 1", gets)
	}
	if starts[0] != 0 {
		t.Fatalf("range start = %d, want 0", starts[0])
	}
}

// TestOpenLTXFile_MultipartLockstep is the head-of-line blocking test. It
// mirrors what ltx.Compactor does during a restore: every file in the plan is
// opened up front and then drained from a single goroutine in round-robin
// order. The pool is sized so that all readers together hold exactly their
// reservations and nothing more, which is the tightest case for deadlock.
func TestOpenLTXFile_MultipartLockstep(t *testing.T) {
	const (
		nreaders = 4
		partSize = 512
		size     = 40000
	)

	servers := make([]*rangeServer, nreaders)
	readers := make([]io.ReadCloser, nreaders)
	pool := newChunkPool(nreaders*minReaderChunks, partSize)

	for i := range nreaders {
		rs, server := newRangeServer(t, size)
		// Stagger the readers so an early one has time to prefetch before the
		// later ones register.
		rs.delay = func(start int64) time.Duration {
			return time.Duration(start%5) * time.Millisecond
		}
		servers[i] = rs

		client := newTestReplicaClient(t, server)
		client.DownloadPartSize = partSize
		client.DownloadConcurrency = nreaders * minReaderChunks
		client.pool = pool

		rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, size)
		if err != nil {
			t.Fatal(err)
		}
		readers[i] = rc
		time.Sleep(5 * time.Millisecond)
	}

	if got, _, _ := poolStats(pool); got != nreaders {
		t.Fatalf("registered readers = %d, want %d", got, nreaders)
	}

	// Drain in lockstep. If any reader could be starved by its peers this
	// never finishes.
	out := make([][]byte, nreaders)
	done := make([]bool, nreaders)
	buf := make([]byte, 333)
	for remaining := nreaders; remaining > 0; {
		for i := range nreaders {
			if done[i] {
				continue
			}
			n, err := readers[i].Read(buf)
			out[i] = append(out[i], buf[:n]...)
			if err == io.EOF {
				done[i], remaining = true, remaining-1
			} else if err != nil {
				t.Fatalf("reader %d: %v", i, err)
			}
		}
	}

	for i := range nreaders {
		if err := readers[i].Close(); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out[i], servers[i].data) {
			t.Fatalf("reader %d: data mismatch (%d bytes)", i, len(out[i]))
		}
	}

	if r, s, _ := poolStats(pool); r != 0 || s != 0 {
		t.Fatalf("pool not drained: readers=%d surplus=%d", r, s)
	}
}

// TestOpenLTXFile_MultipartPoolSaturated verifies that readers beyond the pool's
// capacity fall back to a single stream instead of waiting for a buffer.
func TestOpenLTXFile_MultipartPoolSaturated(t *testing.T) {
	const (
		partSize = 512
		size     = 20000
		poolSize = 4 // room for exactly two registrations
	)

	pool := newChunkPool(poolSize, partSize)
	servers := make([]*rangeServer, 3)
	readers := make([]io.ReadCloser, 3)

	for i := range 3 {
		rs, server := newRangeServer(t, size)
		servers[i] = rs

		client := newTestReplicaClient(t, server)
		client.DownloadPartSize = partSize
		client.DownloadConcurrency = poolSize
		client.pool = pool

		rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, size)
		if err != nil {
			t.Fatal(err)
		}
		readers[i] = rc
	}

	if r, _, _ := poolStats(pool); r != 2 {
		t.Fatalf("registered readers = %d, want 2", r)
	}

	// The third reader could not register, but it keeps the part already in
	// flight: one GET so far, and the continuation only once that is drained.
	if gets, _ := servers[2].stats(); gets != 1 {
		t.Fatalf("fallback reader made %d GETs before reading, want 1", gets)
	}

	for i := range 3 {
		got, err := io.ReadAll(readers[i])
		if err != nil {
			t.Fatalf("reader %d: %v", i, err)
		}
		if err := readers[i].Close(); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, servers[i].data) {
			t.Fatalf("reader %d: data mismatch", i)
		}
	}
}

// TestOpenLTXFile_MultipartOffsetAndSize covers the ranges ResumableReader asks
// for when it reconnects part-way through a file.
func TestOpenLTXFile_MultipartOffsetAndSize(t *testing.T) {
	const (
		partSize = 1024
		size     = 50000
	)

	for _, tt := range []struct {
		offset, size int64
	}{
		{offset: 0, size: 0},
		{offset: 1, size: 0},
		{offset: partSize, size: 0},
		{offset: partSize + 7, size: 0},
		{offset: 0, size: 10000},
		{offset: 4096, size: 20000},
		{offset: 4096, size: partSize},     // single part, exact
		{offset: 4096, size: partSize - 1}, // below one part
		{offset: size - 10, size: 10},      // tail
	} {
		t.Run(fmt.Sprintf("offset=%d/size=%d", tt.offset, tt.size), func(t *testing.T) {
			rs, server := newRangeServer(t, size)
			client, _ := newMultipartTestClient(t, server, 6, partSize)

			rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, tt.offset, tt.size)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatal(err)
			}
			if err := rc.Close(); err != nil {
				t.Fatal(err)
			}

			want := rs.data[tt.offset:]
			if tt.size > 0 {
				want = rs.data[tt.offset : tt.offset+tt.size]
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("got %d bytes, want %d", len(got), len(want))
			}
		})
	}
}

// TestOpenLTXFile_MultipartPartRetry verifies a part whose body dies mid-stream
// is retried in place rather than failing the whole download.
func TestOpenLTXFile_MultipartPartRetry(t *testing.T) {
	const (
		partSize = 1024
		size     = 20000
	)

	for _, name := range []string{"head", "lookahead"} {
		t.Run(name, func(t *testing.T) {
			rs, server := newRangeServer(t, size)
			// "head" breaks chunk 0, which streams live; "lookahead" breaks a
			// pooled chunk fetched in the background.
			if name == "head" {
				rs.truncateOnce[0] = true
			} else {
				rs.truncateOnce[3*partSize] = true
			}

			client, pool := newMultipartTestClient(t, server, 6, partSize)

			rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, size)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatal(err)
			}
			if err := rc.Close(); err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(got, rs.data) {
				t.Fatalf("got %d bytes, want %d", len(got), len(rs.data))
			}
			if r, s, _ := poolStats(pool); r != 0 || s != 0 {
				t.Fatalf("pool not drained: readers=%d surplus=%d", r, s)
			}
		})
	}
}

// TestOpenLTXFile_MultipartCloseReleasesBuffers verifies an abandoned download
// returns every buffer it holds, including in-flight ones.
func TestOpenLTXFile_MultipartCloseReleasesBuffers(t *testing.T) {
	const (
		partSize = 1024
		size     = 200000
		poolSize = 8
	)

	rs, server := newRangeServer(t, size)
	rs.delay = func(int64) time.Duration { return 2 * time.Millisecond }

	client, pool := newMultipartTestClient(t, server, poolSize, partSize)

	rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, size)
	if err != nil {
		t.Fatal(err)
	}

	// Read a little, leaving look-ahead parts in flight, then abandon it.
	if _, err := io.ReadFull(rc, make([]byte, partSize+13)); err != nil {
		t.Fatal(err)
	}
	if r, _, _ := poolStats(pool); r != 1 {
		t.Fatalf("expected the download to be registered, readers=%d", r)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}

	if readers, surplus, _ := poolStats(pool); readers != 0 || surplus != 0 {
		t.Fatalf("pool not drained: readers=%d surplus=%d", readers, surplus)
	}

	// The pool is fully usable again.
	if lease := pool.register(); lease == nil {
		t.Fatal("cannot register after close")
	} else {
		lease.close()
	}
}

// TestChunkPool_RampLeavesRoomToRegister verifies a reader cannot hoard the pool
// before its peers have registered: until it has retired chunks it is held to
// its reservation, so a pool sized for N readers really does admit N.
func TestChunkPool_RampLeavesRoomToRegister(t *testing.T) {
	const readers = 4
	p := newChunkPool(readers*minReaderChunks, 16)

	first := p.register()
	if first == nil {
		t.Fatal("first register failed")
	}

	var held [][]byte
	for {
		buf := first.acquire()
		if buf == nil {
			break
		}
		held = append(held, buf)
	}
	if got, want := len(held), minReaderChunks; got != want {
		t.Fatalf("un-ramped reader held %d buffers, want %d", got, want)
	}

	// The rest of the readers the pool was sized for still fit.
	leases := []*chunkLease{first}
	for i := 1; i < readers; i++ {
		l := p.register()
		if l == nil {
			t.Fatalf("register %d failed with %d buffers outstanding", i+1, len(held))
		}
		leases = append(leases, l)
	}
	if l := p.register(); l != nil {
		t.Fatal("expected registration to fail once reservations are exhausted")
	}

	// Every registered reader can still obtain its reservation.
	for _, l := range leases[1:] {
		for range minReaderChunks {
			if buf := l.acquire(); buf == nil {
				t.Fatal("registered reader could not obtain its reservation")
			} else {
				held = append(held, buf)
			}
		}
	}
	if _, surplus, _ := poolStats(p); surplus != 0 {
		t.Fatalf("surplus=%d, want 0", surplus)
	}
}

// TestChunkPool_WindowOpensAfterFirstChunk verifies a reader is pinned to its
// reservation until it has consumed a chunk, and takes its full share from then
// on -- not a gradual ramp.
func TestChunkPool_WindowOpensAfterFirstChunk(t *testing.T) {
	p := newChunkPool(16, 16)

	l := p.register()
	if l == nil {
		t.Fatal("register failed")
	}

	// share is size/(readers+1) = 8; the window is minReaderChunks, then share.
	for round, want := range []int{minReaderChunks, 8, 8} {
		var held [][]byte
		for {
			buf := l.acquire()
			if buf == nil {
				break
			}
			held = append(held, buf)
		}
		if len(held) != want {
			t.Fatalf("round %d: window = %d, want %d (retired=%d)", round, len(held), want, l.retired)
		}
		// Retire one chunk and reclaim the rest without crediting progress for
		// them, so each round differs only by that single retirement.
		l.release(held[0])
		for _, buf := range held[1:] {
			l.release(buf)
			l.retired--
		}
	}
}

func TestRemainingFromContentRange(t *testing.T) {
	for _, tt := range []struct {
		in     string
		offset int64
		want   int64
		ok     bool
	}{
		{in: "bytes 0-1023/4096", offset: 0, want: 4096, ok: true},
		{in: "bytes 100-1023/4096", offset: 100, want: 3996, ok: true},
		{in: "bytes 0-1023/*", offset: 0, ok: false},
		{in: "", offset: 0, ok: false},
		{in: "bytes 0-1023/100", offset: 100, ok: false},
	} {
		got, ok := remainingFromContentRange(tt.in, tt.offset)
		if ok != tt.ok || (ok && got != tt.want) {
			t.Fatalf("remainingFromContentRange(%q, %d) = (%d, %v), want (%d, %v)", tt.in, tt.offset, got, ok, tt.want, tt.ok)
		}
	}
}

// TestNewReplicaClientFromURL_DownloadOptions verifies the multipart download
// settings are readable from the replica URL and default when absent.
func TestNewReplicaClientFromURL_DownloadOptions(t *testing.T) {
	parse := func(t *testing.T, query string) *ReplicaClient {
		t.Helper()

		u, err := url.Parse("s3://mybucket/db" + query)
		if err != nil {
			t.Fatal(err)
		}
		c, err := NewReplicaClientFromURL(u.Scheme, u.Host, strings.TrimPrefix(u.Path, "/"), u.Query(), nil)
		if err != nil {
			t.Fatal(err)
		}
		return c.(*ReplicaClient)
	}

	t.Run("Defaults", func(t *testing.T) {
		client := parse(t, "")
		if got, want := client.DownloadPartSize, int64(DefaultDownloadPartSize); got != want {
			t.Errorf("DownloadPartSize = %d, want %d", got, want)
		}
		if got, want := client.DownloadConcurrency, DefaultDownloadConcurrency; got != want {
			t.Errorf("DownloadConcurrency = %d, want %d", got, want)
		}
	})

	t.Run("Overrides", func(t *testing.T) {
		client := parse(t, "?downloadPartSize=2097152&downloadConcurrency=9")
		if got, want := client.DownloadPartSize, int64(2097152); got != want {
			t.Errorf("DownloadPartSize = %d, want %d", got, want)
		}
		if got, want := client.DownloadConcurrency, 9; got != want {
			t.Errorf("DownloadConcurrency = %d, want %d", got, want)
		}
	})

	t.Run("Disabled", func(t *testing.T) {
		if got := parse(t, "?download-concurrency=0").DownloadConcurrency; got != 0 {
			t.Errorf("DownloadConcurrency = %d, want 0", got)
		}
	})
}

// TestOpenLTXFile_MultipartOversizedRequest verifies that a caller asking for
// more bytes than the object holds fails loudly.
//
// Callers take the size from the replica listing and LTX files are immutable,
// so an overrun means the listing and the object disagree. Clamping would hand
// back known-incomplete LTX data and resurface as an unrelated decode error
// somewhere downstream; see docs/PATTERNS.md on returning errors that affect
// correctness.
func TestOpenLTXFile_MultipartOversizedRequest(t *testing.T) {
	const (
		partSize = 1024
		size     = 10000
	)

	_, server := newRangeServer(t, size)
	client, pool := newMultipartTestClient(t, server, 6, partSize)

	rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, size*4)
	if err == nil {
		_ = rc.Close()
		t.Fatal("expected an error when more bytes are requested than the object holds")
	}
	for _, want := range []string{"requested 40000 bytes", "offset 0", "object holds 10000"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %q", err, want)
		}
	}
	if r, _, free := poolStats(pool); r != 0 || free != 0 {
		t.Fatalf("pool touched on the failure path: readers=%d free=%d", r, free)
	}
}

// TestOpenLTXFile_OversizedRequestOnSingleGetPaths verifies the same check binds
// on the reads that never reach the multipart planner: a short object is just as
// wrong when multipart is off, or when the caller's size fits in one part.
func TestOpenLTXFile_OversizedRequestOnSingleGetPaths(t *testing.T) {
	const (
		partSize = 1024
		size     = 100
	)

	for _, tt := range []struct {
		name        string
		concurrency int
		request     int64
	}{
		{name: "MultipartDisabled", concurrency: 0, request: partSize},
		{name: "FitsInOnePart", concurrency: 6, request: partSize},
		{name: "SmallerThanPart", concurrency: 6, request: size * 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, server := newRangeServer(t, size)

			client := newTestReplicaClient(t, server)
			client.DownloadPartSize = partSize
			client.DownloadConcurrency = tt.concurrency

			rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, tt.request)
			if err == nil {
				_ = rc.Close()
				t.Fatal("expected an error when more bytes are requested than the object holds")
			}
			if !strings.Contains(err.Error(), "object holds 100") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestOpenLTXFile_DownloadConcurrencyBelowMinimum verifies that a pool too small
// to seat a single reader is treated as disabled, rather than making every large
// read pay for a part-sized probe it can never use.
func TestOpenLTXFile_DownloadConcurrencyBelowMinimum(t *testing.T) {
	const (
		partSize = 1024
		size     = 20000
	)

	for _, concurrency := range []int{0, 1} {
		t.Run(strconv.Itoa(concurrency), func(t *testing.T) {
			rs, server := newRangeServer(t, size)

			client := newTestReplicaClient(t, server)
			client.DownloadPartSize = partSize
			client.DownloadConcurrency = concurrency

			rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, size)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatal(err)
			}
			if err := rc.Close(); err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(got, rs.data) {
				t.Fatalf("got %d bytes, want %d", len(got), len(rs.data))
			}
			gets, starts := rs.stats()
			if gets != 1 {
				t.Fatalf("GET count = %d, want 1 (multipart disabled, no probe): %v", gets, starts)
			}
		})
	}
}

// TestOpenLTXFile_MultipartPoolGeometryWins verifies the pool's part size is
// authoritative. A client asking for a larger part than the pool's buffers can
// hold must not be able to overrun one.
func TestOpenLTXFile_MultipartPoolGeometryWins(t *testing.T) {
	const (
		poolPartSize = 1024
		size         = 20000
	)

	for _, clientPartSize := range []int64{poolPartSize / 4, poolPartSize * 8} {
		t.Run(strconv.FormatInt(clientPartSize, 10), func(t *testing.T) {
			rs, server := newRangeServer(t, size)

			client := newTestReplicaClient(t, server)
			client.DownloadConcurrency = 6
			client.DownloadPartSize = clientPartSize
			client.pool = newChunkPool(6, poolPartSize)

			rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, size)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatal(err)
			}
			if err := rc.Close(); err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(got, rs.data) {
				t.Fatalf("got %d bytes, want %d", len(got), len(rs.data))
			}
			// Every request must be sized by the pool, not the client.
			_, starts := rs.stats()
			for i, start := range starts {
				if start%poolPartSize != 0 {
					t.Fatalf("request %d started at %d, not a pool part boundary", i, start)
				}
			}
		})
	}
}

// TestOpenLTXFile_MultipartRetryBudget verifies that a part which never
// completes gives up with an error rather than retrying forever. The retry loops
// reset their budget on forward progress, so an unbounded stream of
// zero-progress failures is the case that must terminate.
func TestOpenLTXFile_MultipartRetryBudget(t *testing.T) {
	const (
		partSize = 1024
		size     = 20000
	)

	restore := downloadPartBackoff
	downloadPartBackoff = time.Millisecond
	t.Cleanup(func() { downloadPartBackoff = restore })

	for _, name := range []string{"head", "lookahead"} {
		t.Run(name, func(t *testing.T) {
			rs, server := newRangeServer(t, size)
			// "head" starves chunk 0, which streams live; "lookahead" starves a
			// pooled chunk fetched in the background. Neither can ever finish.
			broken := int64(0)
			if name == "lookahead" {
				broken = 3 * partSize
			}
			rs.emptyBody[broken] = true

			client, pool := newMultipartTestClient(t, server, 6, partSize)

			rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, size)
			if err != nil {
				t.Fatal(err)
			}

			done := make(chan error, 1)
			go func() {
				_, err := io.ReadAll(rc)
				done <- err
			}()

			select {
			case err := <-done:
				if err == nil {
					t.Fatal("expected an error once the retry budget is exhausted")
				}
				if !strings.Contains(err.Error(), "download part") {
					t.Fatalf("unexpected error: %v", err)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("read did not terminate: the retry budget is not bounded")
			}

			gets, _ := rs.stats()
			if gets > 2*downloadPartAttempts {
				t.Fatalf("made %d requests for a part that never progresses; the budget is not binding", gets)
			}

			if err := rc.Close(); err != nil {
				t.Fatal(err)
			}
			if r, s, _ := poolStats(pool); r != 0 || s != 0 {
				t.Fatalf("pool not drained after failure: readers=%d surplus=%d", r, s)
			}
		})
	}
}

// TestOpenLTXFile_MultipartDribbleBudget verifies a backend that returns a
// little data per connection cannot make a chunk open connections without
// bound. Resetting the retry budget on any forward progress would let it open
// one connection per byte, which is the churn benbjohnson/litestream#1500
// identifies in the resumable reader; the budget is a lifetime count instead.
func TestOpenLTXFile_MultipartDribbleBudget(t *testing.T) {
	const (
		partSize = 4096
		size     = 40000
	)

	restore := downloadPartBackoff
	downloadPartBackoff = time.Millisecond
	t.Cleanup(func() { downloadPartBackoff = restore })

	rs, server := newRangeServer(t, size)
	// Every response after the first part carries a single byte, so each
	// connection advances the chunk by one byte and never completes it.
	rs.maxBody = 1

	client, pool := newMultipartTestClient(t, server, 6, partSize)

	rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, size)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(rc)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an error once the attempt budget is exhausted")
		}
		if !strings.Contains(err.Error(), "download part") {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("read did not terminate: the attempt budget is not bounded")
	}

	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}

	// A budget that reset on progress would open one connection per byte.
	if gets, _ := rs.stats(); gets > 4*downloadPartAttempts {
		t.Fatalf("made %d requests for a dribbling chunk; the budget is not a lifetime count", gets)
	}
	if r, s, _ := poolStats(pool); r != 0 || s != 0 {
		t.Fatalf("pool not drained: readers=%d surplus=%d", r, s)
	}
}

// TestOpenLTXFile_MultipartCloseIsTerminal verifies a read after Close reports
// the close rather than surfacing the cancelled context as a download failure.
func TestOpenLTXFile_MultipartCloseIsTerminal(t *testing.T) {
	const (
		partSize = 1024
		size     = 20000
	)

	_, server := newRangeServer(t, size)
	client, _ := newMultipartTestClient(t, server, 6, partSize)

	rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, size)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := rc.Read(make([]byte, 64)); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("read after close = %v, want os.ErrClosed", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("second close = %v, want nil", err)
	}
}
