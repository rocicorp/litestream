package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/benbjohnson/litestream/internal"
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

	// omitContentRange simulates a provider that does not echo Content-Range on
	// a ranged GET.
	omitContentRange bool

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

	rs := &rangeServer{data: data, truncateOnce: make(map[int64]bool)}
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
	rs.mu.Unlock()

	if rs.delay != nil {
		time.Sleep(rs.delay(start))
	}

	body := rs.data[start : end+1]
	w.Header().Set("Accept-Ranges", "bytes")
	if !rs.omitContentRange {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	}
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

func poolStats(p *chunkPool) (readers, surplus, allocated, free int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.readers, p.surplus, p.allocated, len(p.free)
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

			rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
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

			readers, surplus, allocated, free := poolStats(pool)
			if readers != 0 || surplus != 0 {
				t.Fatalf("pool not drained: readers=%d surplus=%d", readers, surplus)
			}
			if allocated > tt.poolSize {
				t.Fatalf("allocated %d buffers, pool size is %d", allocated, tt.poolSize)
			}
			if free != allocated {
				t.Fatalf("free=%d, want all %d buffers returned", free, allocated)
			}
		})
	}
}

// TestOpenLTXFile_SmallObjectSingleGet verifies the size probe costs nothing
// for objects that fit in one part: a single GET and no pool buffers.
func TestOpenLTXFile_SmallObjectSingleGet(t *testing.T) {
	const partSize = 4096

	for _, size := range []int{1, 100, partSize - 1, partSize} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			rs, server := newRangeServer(t, size)
			client, pool := newMultipartTestClient(t, server, 8, partSize)

			rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
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
			if readers, _, allocated, _ := poolStats(pool); readers != 0 || allocated != 0 {
				t.Fatalf("pool touched for small object: readers=%d allocated=%d", readers, allocated)
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

		rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		readers[i] = rc
		time.Sleep(5 * time.Millisecond)
	}

	if got, _, _, _ := poolStats(pool); got != nreaders {
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

	if r, s, _, _ := poolStats(pool); r != 0 || s != 0 {
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

		rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		readers[i] = rc
	}

	if r, _, _, _ := poolStats(pool); r != 2 {
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

			rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
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
			if r, s, _, _ := poolStats(pool); r != 0 || s != 0 {
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

	rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Read a little, leaving look-ahead parts in flight, then abandon it.
	if _, err := io.ReadFull(rc, make([]byte, partSize+13)); err != nil {
		t.Fatal(err)
	}
	if _, _, allocated, _ := poolStats(pool); allocated == 0 {
		t.Fatal("expected look-ahead buffers to be in use")
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}

	readers, surplus, allocated, free := poolStats(pool)
	if readers != 0 || surplus != 0 {
		t.Fatalf("pool not drained: readers=%d surplus=%d", readers, surplus)
	}
	if free != allocated {
		t.Fatalf("free=%d allocated=%d, want every buffer returned", free, allocated)
	}

	// The pool is fully usable again.
	if lease := pool.register(); lease == nil {
		t.Fatal("cannot register after close")
	} else {
		lease.close()
	}
}

// TestOpenLTXFile_MultipartMetrics verifies every part GET is recorded.
func TestOpenLTXFile_MultipartMetrics(t *testing.T) {
	const (
		partSize = 1024
		size     = 16 * 1024
	)

	rs, server := newRangeServer(t, size)
	client, _ := newMultipartTestClient(t, server, 6, partSize)

	countBefore := testutil.ToFloat64(internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET"))
	bytesBefore := testutil.ToFloat64(internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET"))

	rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}

	gets, _ := rs.stats()
	if gets != size/partSize {
		t.Fatalf("GET count = %d, want %d", gets, size/partSize)
	}

	countAfter := testutil.ToFloat64(internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET"))
	bytesAfter := testutil.ToFloat64(internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET"))

	if got, want := countAfter-countBefore, float64(gets); got != want {
		t.Fatalf("GET counter delta = %v, want %v", got, want)
	}
	if got, want := bytesAfter-bytesBefore, float64(size); got != want {
		t.Fatalf("GET bytes delta = %v, want %v", got, want)
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
	if _, surplus, allocated, _ := poolStats(p); surplus != 0 || allocated != readers*minReaderChunks {
		t.Fatalf("surplus=%d allocated=%d, want 0 and %d", surplus, allocated, readers*minReaderChunks)
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
	if r, _, allocated, _ := poolStats(pool); r != 0 || allocated != 0 {
		t.Fatalf("pool touched on the failure path: readers=%d allocated=%d", r, allocated)
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

			rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
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

			rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
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

// TestOpenLTXFile_MultipartSizeFromCaller verifies that when the caller knows
// the size -- which every restore path now passes down -- the download plans
// itself without relying on the provider echoing Content-Range, and without the
// extra unbounded GET that the probe path needs to recover.
func TestOpenLTXFile_MultipartSizeFromCaller(t *testing.T) {
	const (
		partSize = 1024
		size     = 20000
	)

	t.Run("SizeKnown", func(t *testing.T) {
		rs, server := newRangeServer(t, size)
		rs.omitContentRange = true

		client, _ := newMultipartTestClient(t, server, 6, partSize)

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
		// Exactly one GET per part: no probe recovery, no re-reads.
		gets, _ := rs.stats()
		if want := (size + partSize - 1) / partSize; gets != want {
			t.Fatalf("GET count = %d, want %d", gets, want)
		}
	})

	t.Run("SizeUnknownContinuesRatherThanRefetching", func(t *testing.T) {
		rs, server := newRangeServer(t, size)
		rs.omitContentRange = true

		client, pool := newMultipartTestClient(t, server, 6, partSize)

		rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
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

		// Without Content-Range there is no length to plan a parallel download
		// from, but the part already in flight is still good data: the read
		// continues from where it stopped instead of starting over.
		gets, starts := rs.stats()
		if gets != 2 {
			t.Fatalf("GET count = %d, want 2 (first part + continuation): %v", gets, starts)
		}
		if want := []int64{0, partSize}; starts[0] != want[0] || starts[1] != want[1] {
			t.Fatalf("request offsets = %v, want %v -- the first part must not be re-fetched", starts, want)
		}
		if r, _, allocated, _ := poolStats(pool); r != 0 || allocated != 0 {
			t.Fatalf("pool used on the unplanned path: readers=%d allocated=%d", r, allocated)
		}
	})

	// An object whose length is an exact multiple of the part size is the
	// ambiguous case: the first response is full-length, but the object may or
	// may not continue. The continuation settles it either way, and because it
	// is unbounded it costs the same two requests regardless -- what differs is
	// whether the second returns data or 416.
	t.Run("SizeUnknownExactPartBoundary", func(t *testing.T) {
		for _, tt := range []struct {
			parts      int
			wantStarts []int64 // a 416 records no start
		}{
			{parts: 1, wantStarts: []int64{0}},           // nothing follows: 416
			{parts: 2, wantStarts: []int64{0, partSize}}, // continues to EOF
			{parts: 5, wantStarts: []int64{0, partSize}},
		} {
			t.Run(strconv.Itoa(tt.parts), func(t *testing.T) {
				rs, server := newRangeServer(t, tt.parts*partSize)
				rs.omitContentRange = true

				client, _ := newMultipartTestClient(t, server, 6, partSize)

				rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
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
				if gets != 2 {
					t.Fatalf("GET count = %d, want 2 (first part + continuation): %v", gets, starts)
				}
				if len(starts) != len(tt.wantStarts) {
					t.Fatalf("served ranges = %v, want %v", starts, tt.wantStarts)
				}
				for i := range starts {
					if starts[i] != tt.wantStarts[i] {
						t.Fatalf("served ranges = %v, want %v", starts, tt.wantStarts)
					}
				}
			})
		}
	})
}
