package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/benbjohnson/litestream/internal"
)

// Default multipart download settings.
//
// A single S3 GET stream is bandwidth-limited well below what a modern instance
// can pull, so large LTX files are fetched as several ranged GETs in parallel and
// reassembled in order. Up to DefaultDownloadConcurrency * DefaultDownloadPartSize
// bytes (768MB) of chunk data may be buffered process-wide.
const (
	DefaultDownloadPartSize    = 16 * 1024 * 1024
	DefaultDownloadConcurrency = 48
)

// minReaderChunks is the number of chunk buffers reserved for each registered
// multipart reader: one being drained by the consumer and one being fetched.
//
// Reserving them at registration is what makes the shared pool deadlock-free.
// The LTX compactor opens every file in a restore plan up front and then merges
// them page-by-page from a single goroutine, so all readers advance in lockstep
// and a reader that cannot obtain a buffer stalls the entire restore. Because a
// reservation can never be taken by another reader, every registered reader can
// always make forward progress without waiting on one of its peers.
const minReaderChunks = 2

// downloadPartRetries bounds the retries for a single chunk that is making no
// forward progress. The budget resets whenever bytes are received, so a
// connection that drops repeatedly but keeps advancing is not limited by it.
const downloadPartRetries = 3

// chunkPool is a bounded set of fixed-size buffers shared by every multipart
// download in the process. Buffers are allocated on first use and recycled, so a
// process that never downloads a large file never allocates any.
type chunkPool struct {
	mu        sync.Mutex
	size      int      // maximum number of live buffers
	partSize  int64    // size of each buffer
	readers   int      // registered readers; readers*minReaderChunks slots are reserved
	surplus   int      // slots granted beyond the per-reader reservations
	allocated int      // buffers ever created; bounded by size
	free      [][]byte // idle buffers available for reuse
}

func newChunkPool(size int, partSize int64) *chunkPool {
	return &chunkPool{size: size, partSize: partSize}
}

// register reserves minReaderChunks slots for one reader. It returns nil when
// the pool cannot cover another reservation, in which case the caller must fall
// back to a single unbounded GET rather than wait.
func (p *chunkPool) register() *chunkLease {
	p.mu.Lock()
	defer p.mu.Unlock()

	if (p.readers+1)*minReaderChunks+p.surplus > p.size {
		return nil
	}
	p.readers++
	return &chunkLease{pool: p}
}

// shareLocked returns the maximum number of buffers a single reader may hold.
//
// The divisor includes a phantom reader so that a lone reader can never consume
// the whole pool; there is always room left for the next file in the restore
// plan to register.
func (p *chunkPool) shareLocked() int {
	share := p.size / (p.readers + 1)
	if share < minReaderChunks {
		share = minReaderChunks
	}
	return share
}

// rampLocked holds a reader to its reservation until it has consumed a chunk,
// after which it may take its full share.
//
// This exists to keep a reader from hoarding the pool before its peers have
// registered. Readers register as the compactor decodes each input's header, so
// they all arrive before any of them has retired a chunk -- decoding a header
// reads a few hundred bytes, not a whole part. Pinning the window to the
// reservation for exactly that long is enough, and costs one chunk of reduced
// look-ahead rather than several.
//
// It only bites when the pool is tight relative to the number of readers. At
// the default size the shareLocked phantom-reader divisor already guarantees a
// newcomer can register even against a reader at full width; a small pool (see
// TestChunkPool_RampLeavesRoomToRegister) is where this matters.
func (l *chunkLease) rampLocked() int {
	if l.retired == 0 {
		return minReaderChunks
	}
	return l.pool.size // no longer limiting; shareLocked governs
}

func (p *chunkPool) takeLocked() []byte {
	if n := len(p.free); n > 0 {
		buf := p.free[n-1]
		p.free[n-1] = nil
		p.free = p.free[:n-1]
		return buf
	}
	p.allocated++
	return make([]byte, p.partSize)
}

// chunkLease is one reader's claim on the pool.
type chunkLease struct {
	pool    *chunkPool
	held    int
	retired int // chunks consumed; widens the reader's window
	closed  bool
}

// acquire returns a buffer, or nil when the reader is already at its share or
// the pool has no spare slot. It never blocks.
func (l *chunkLease) acquire() []byte {
	p := l.pool

	p.mu.Lock()
	defer p.mu.Unlock()

	if l.closed || l.held >= min(p.shareLocked(), l.rampLocked()) {
		return nil
	}
	// Beyond its reservation a reader competes for whatever is left over.
	if l.held >= minReaderChunks {
		if p.readers*minReaderChunks+p.surplus >= p.size {
			return nil
		}
		p.surplus++
	}
	l.held++
	return p.takeLocked()
}

func (l *chunkLease) release(buf []byte) {
	p := l.pool

	p.mu.Lock()
	defer p.mu.Unlock()

	l.held--
	l.retired++
	if l.held >= minReaderChunks {
		p.surplus--
	}
	p.free = append(p.free, buf)
}

// close drops the reservation. All acquired buffers must be released first.
func (l *chunkLease) close() {
	p := l.pool

	p.mu.Lock()
	defer p.mu.Unlock()

	if l.closed {
		return
	}
	l.closed = true
	if l.held > minReaderChunks {
		p.surplus -= l.held - minReaderChunks
	}
	l.held = 0
	p.readers--
}

var (
	sharedPoolOnce sync.Once
	sharedPool     *chunkPool
)

// sharedChunkPool returns the process-wide pool, sized by the first client to
// ask for it. The budget is deliberately global: a litestream process can
// replicate many databases and each would otherwise carry its own
// multi-hundred-megabyte pool.
//
// The pool owns the part size so a buffer can never be smaller than the chunk
// written into it. A second client configured differently therefore inherits the
// established geometry; see ReplicaClient.downloadPool.
func sharedChunkPool(size int, partSize int64) *chunkPool {
	sharedPoolOnce.Do(func() {
		sharedPool = newChunkPool(size, partSize)
	})
	return sharedPool
}

// downloadChunk is a single ranged GET in flight or waiting to be consumed.
type downloadChunk struct {
	idx  int64
	buf  []byte
	n    int
	err  error
	done chan struct{}
}

// multipartReader reassembles an object from parallel ranged GETs.
//
// Chunks are requested strictly in increasing index order, so head-of-line
// blocking within a file is impossible by construction: the buffer a reader is
// granted always goes to the chunk the consumer needs next.
//
// Chunk 0 is streamed straight from the response that opened the download. It is
// the head of the line by definition, so there is nothing to reorder and it never
// occupies a pool buffer.
//
// A multipartReader is not safe for concurrent use.
type multipartReader struct {
	c      *ReplicaClient
	key    string
	ctx    context.Context
	cancel context.CancelFunc
	lease  *chunkLease
	wg     sync.WaitGroup

	base     int64 // absolute object offset of chunk 0
	total    int64 // bytes to deliver starting at base
	partSize int64
	lastIdx  int64

	// Chunk 0 state. Only touched by the reading goroutine.
	live      io.ReadCloser
	livePos   int64
	liveTries int

	mu        sync.Mutex
	pending   map[int64]*downloadChunk
	nextFetch int64
	nextRead  int64
	cur       *downloadChunk
	curOff    int
	err       error
	closed    bool
}

func newMultipartReader(ctx context.Context, c *ReplicaClient, key string, live io.ReadCloser, base, total, partSize int64, lease *chunkLease) *multipartReader {
	ctx, cancel := context.WithCancel(ctx)

	r := &multipartReader{
		c:         c,
		key:       key,
		ctx:       ctx,
		cancel:    cancel,
		lease:     lease,
		base:      base,
		total:     total,
		partSize:  partSize,
		lastIdx:   (total - 1) / partSize,
		live:      live,
		pending:   make(map[int64]*downloadChunk),
		nextFetch: 1,
		nextRead:  1,
	}

	r.mu.Lock()
	r.schedule()
	r.mu.Unlock()

	return r
}

// schedule starts as many look-ahead chunks as the pool currently allows.
// r.mu must be held.
func (r *multipartReader) schedule() {
	for !r.closed && r.nextFetch <= r.lastIdx {
		buf := r.lease.acquire()
		if buf == nil {
			return
		}
		ch := &downloadChunk{idx: r.nextFetch, buf: buf, done: make(chan struct{})}
		r.pending[ch.idx] = ch
		r.nextFetch++
		r.wg.Add(1)
		go r.fetch(ch)
	}
}

// fetch downloads one chunk into its buffer, retrying in place so a transient
// failure does not discard the progress of the whole download.
func (r *multipartReader) fetch(ch *downloadChunk) {
	defer r.wg.Done()
	defer close(ch.done)

	start := r.base + ch.idx*r.partSize
	length := r.partSize
	if end := r.base + r.total; start+length > end {
		length = end - start
	}

	var off int64
	for tries := 0; ; {
		n, err := r.c.readRange(r.ctx, r.key, start+off, length-off, ch.buf[off:length])
		off += n
		if off >= length {
			ch.n = int(length)
			return
		}
		if n > 0 {
			tries = 0 // forward progress
		}
		if r.ctx.Err() != nil {
			ch.err = fmt.Errorf("s3: download part %d of %s: %w", ch.idx, r.key, r.ctx.Err())
			return
		}
		if errors.Is(err, os.ErrNotExist) {
			ch.err = err
			return
		}
		if tries++; tries > downloadPartRetries {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			ch.err = fmt.Errorf("s3: download part %d of %s at offset %d: %w", ch.idx, r.key, start+off, err)
			return
		}
	}
}

func (r *multipartReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	// Chunk 0 streams directly from the response that opened the download.
	if r.livePos < r.partSize {
		n, err := r.readLive(p)
		if n > 0 || err != nil {
			return n, err
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.err != nil {
		return 0, r.err
	}

	for {
		if r.cur == nil {
			if r.nextRead > r.lastIdx {
				return 0, io.EOF
			}
			ch := r.pending[r.nextRead]
			if ch == nil {
				// The pool had nothing to spare when the previous chunk was
				// retired; the reservation guarantees a slot is available now.
				r.schedule()
				if ch = r.pending[r.nextRead]; ch == nil {
					r.err = fmt.Errorf("s3: download part %d of %s: no buffer available", r.nextRead, r.key)
					return 0, r.err
				}
			}

			r.mu.Unlock()
			<-ch.done
			r.mu.Lock()

			if r.closed {
				return 0, os.ErrClosed
			}
			if ch.err != nil {
				r.err = ch.err
				return 0, r.err
			}
			r.cur, r.curOff = ch, 0
		}

		if r.curOff < r.cur.n {
			n := copy(p, r.cur.buf[r.curOff:r.cur.n])
			r.curOff += n
			if r.curOff >= r.cur.n {
				r.retire()
			}
			return n, nil
		}
		r.retire()
	}
}

// retire returns the current chunk's buffer to the pool and refills the window.
// r.mu must be held.
func (r *multipartReader) retire() {
	delete(r.pending, r.cur.idx)
	r.lease.release(r.cur.buf)
	r.cur, r.curOff = nil, 0
	r.nextRead++
	r.schedule()
}

// readLive serves chunk 0 from its live response body, reconnecting from the
// current offset if the stream breaks.
func (r *multipartReader) readLive(p []byte) (int, error) {
	for {
		remain := r.partSize - r.livePos
		if remain <= 0 {
			return 0, nil
		}
		if int64(len(p)) > remain {
			p = p[:remain]
		}

		if r.live == nil {
			rc, err := r.c.getRange(r.ctx, r.key, r.base+r.livePos, remain)
			if err != nil {
				if failed := r.liveFailed(err); failed != nil {
					return 0, failed
				}
				continue
			}
			r.live = rc
		}

		n, err := r.live.Read(p)
		r.livePos += int64(n)
		if n > 0 {
			r.liveTries = 0
		}
		if r.livePos >= r.partSize {
			r.closeLive()
		}

		if err == nil || (err == io.EOF && r.livePos >= r.partSize) {
			if n > 0 {
				return n, nil
			}
			if err != nil {
				return 0, nil // chunk 0 fully delivered
			}
			continue
		}

		// Broken or prematurely closed stream: resume from the current offset.
		r.closeLive()
		failed := r.liveFailed(err)
		if n > 0 {
			return n, nil
		}
		if failed != nil {
			return 0, failed
		}
	}
}

// liveFailed records a chunk 0 failure and returns a terminal error once the
// retry budget for the current offset is exhausted.
func (r *multipartReader) liveFailed(err error) error {
	if r.ctx.Err() != nil || errors.Is(err, os.ErrNotExist) {
		r.setErr(fmt.Errorf("s3: download part 0 of %s at offset %d: %w", r.key, r.base+r.livePos, err))
		return r.readErr()
	}
	if r.liveTries++; r.liveTries > downloadPartRetries {
		r.setErr(fmt.Errorf("s3: download part 0 of %s at offset %d: %w", r.key, r.base+r.livePos, err))
		return r.readErr()
	}
	return nil
}

func (r *multipartReader) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err == nil {
		r.err = err
	}
}

func (r *multipartReader) readErr() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *multipartReader) closeLive() {
	if r.live != nil {
		_ = r.live.Close()
		r.live = nil
	}
}

func (r *multipartReader) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()

	// Abort in-flight requests, then wait so no goroutine can write into a
	// buffer after it has gone back to the pool.
	r.cancel()
	r.wg.Wait()
	r.closeLive()

	r.mu.Lock()
	if r.cur != nil {
		r.pending[r.cur.idx] = r.cur
		r.cur = nil
	}
	for idx, ch := range r.pending {
		delete(r.pending, idx)
		r.lease.release(ch.buf)
	}
	r.mu.Unlock()

	r.lease.close()
	return nil
}

// remainingFromContentRange returns the number of bytes from offset to the end
// of the object, given a "bytes <start>-<end>/<total>" response header.
func remainingFromContentRange(v string, offset int64) (int64, bool) {
	i := strings.LastIndex(v, "/")
	if i < 0 {
		return 0, false
	}
	total, err := strconv.ParseInt(strings.TrimSpace(v[i+1:]), 10, 64)
	if err != nil || total <= offset {
		return 0, false
	}
	return total - offset, true
}

// recordGet records a completed GET request against the operation metrics.
func recordGet(n int64) {
	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()
	if n > 0 {
		internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET").Add(float64(n))
	}
}

// continuationReader serves one body and then continues from the byte that
// follows it, opening the continuation only once the first body is drained.
//
// It exists for providers that omit Content-Range on a ranged GET. The object
// length is unknown, so a parallel download cannot be planned, but the part
// already in flight is still valid data and must not be thrown away. Whether
// anything follows it is answered by the continuation itself: bytes mean the
// object continues, and 416 means it ended exactly on the part boundary.
type continuationReader struct {
	c   *ReplicaClient
	ctx context.Context
	key string

	rc      io.ReadCloser
	nextOff int64 // start of the continuation; negative once it has been opened
}

func (r *continuationReader) Read(p []byte) (int, error) {
	for {
		if r.rc != nil {
			n, err := r.rc.Read(p)
			if n > 0 {
				return n, nil
			}
			if err == nil {
				continue
			}
			if err != io.EOF {
				return 0, err
			}
			_ = r.rc.Close()
			r.rc = nil
		}

		if r.nextOff < 0 {
			return 0, io.EOF
		}
		off := r.nextOff
		r.nextOff = -1

		rc, err := r.c.getRange(r.ctx, r.key, off, 0)
		if err != nil {
			if isRangeNotSatisfiable(err) {
				// Nothing follows: the object ended on the part boundary.
				return 0, io.EOF
			}
			return 0, err
		}
		r.rc = rc
	}
}

func (r *continuationReader) Close() error {
	if r.rc != nil {
		return r.rc.Close()
	}
	return nil
}
