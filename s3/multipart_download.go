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
	"time"
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

// minDownloadConcurrency is the smallest pool worth having. One buffer would
// add a single connection of look-ahead to a stream that is already live, which
// is not a parallel download, so anything below two disables multipart.
const minDownloadConcurrency = 2

// downloadPartAttempts bounds the connections opened for a single chunk.
//
// This is a lifetime budget rather than a consecutive-failure count that resets
// on any forward progress. A backend that answers with one byte per connection
// would otherwise have a chunk open one connection per byte and never let
// backoff engage -- the churn that benbjohnson/litestream#1500 identifies in the
// resumable reader. Six attempts still absorbs several disconnects in a chunk
// that is otherwise progressing, since each attempt resumes from the last byte
// received rather than restarting the chunk.
const downloadPartAttempts = 6

// downloadPartBackoff is the base delay between attempts, doubling each time.
// Retrying with no delay lands every attempt inside the same provider throttle
// window (e.g. Tigris 408 load shedding), spending the budget without giving the
// provider a chance to recover. A var so tests can shorten it.
var downloadPartBackoff = 250 * time.Millisecond

// chunkPool is a bounded set of fixed-size buffers shared by every multipart
// download in the process. Buffers are allocated on first use and recycled, so a
// process that never downloads a large file never allocates any.
//
// Buffers are look-ahead, not a requirement. A reader that holds no buffer for
// the chunk it needs next streams that chunk from a GET of its own, the way
// chunk 0 is always served, so no reader ever waits on the pool. That is what
// keeps a shared pool deadlock-free: the LTX compactor opens every file in a
// restore plan and drains them page-by-page from one goroutine, so a reader
// blocked on a buffer held by a peer would be waiting on the very goroutine
// that is blocked in it.
//
// The pool is divided among registered readers in proportion to how much of
// each file is still to be read; see shareLocked.
type chunkPool struct {
	mu        sync.Mutex
	size      int      // maximum number of live buffers
	partSize  int64    // size of each buffer
	readers   int      // registered readers
	held      int      // buffers currently lent out
	remaining int64    // chunks still to be delivered, summed over registered readers
	free      [][]byte // idle buffers available for reuse
}

func newChunkPool(size int, partSize int64) *chunkPool {
	return &chunkPool{size: size, partSize: partSize}
}

// register admits a reader that will deliver the given number of chunks. It
// never refuses: a reader the pool cannot serve streams instead of waiting.
func (p *chunkPool) register(chunks int64) *chunkLease {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.readers++
	p.remaining += chunks
	return &chunkLease{pool: p, remaining: chunks}
}

// shareLocked returns the number of buffers a reader may hold right now: its
// remaining chunks as a fraction of everyone's, applied to the whole pool.
//
// Why proportional. The compactor merges its inputs by page number, so it draws
// on every input at a rate proportional to that input's size and keeps all of
// them open until the last page: a 60 GiB snapshot and a 50 MiB incremental are
// drained side by side for the whole restore, one fast and one at a trickle.
// Sharing by remaining bytes gives every reader a window that empties at the
// same rate as its peers'. An equal split would give the snapshot -- nearly all
// of the bytes -- the same handful of buffers as a file the compactor barely
// touches, and with a dozen inputs the restore runs at two connections.
//
// Remaining rather than total so that shares follow the work: as inputs finish,
// their buffers flow to the ones still going, and a lone reader ends up with the
// whole pool.
//
// The share is rounded up so no reader with work left is denied look-ahead
// entirely. Rounded shares can sum to more than the pool, but acquire applies
// the pool's own bound first, so the excess only decides who wins a free buffer.
func (p *chunkPool) shareLocked(l *chunkLease) int {
	if l.remaining <= 0 || p.remaining <= 0 {
		return 0
	}
	return int((int64(p.size)*l.remaining + p.remaining - 1) / p.remaining)
}

func (p *chunkPool) takeLocked() []byte {
	if n := len(p.free); n > 0 {
		buf := p.free[n-1]
		p.free[n-1] = nil
		p.free = p.free[:n-1]
		return buf
	}
	return make([]byte, p.partSize)
}

// chunkLease is one reader's claim on the pool.
type chunkLease struct {
	pool      *chunkPool
	held      int
	remaining int64 // chunks not yet delivered, including any in flight
	closed    bool
}

// acquire returns a buffer, or nil when the reader is already at its share or
// the pool has none spare. It never blocks.
func (l *chunkLease) acquire() []byte {
	p := l.pool

	p.mu.Lock()
	defer p.mu.Unlock()

	if l.closed || p.held >= p.size || l.held >= p.shareLocked(l) {
		return nil
	}
	p.held++
	l.held++
	return p.takeLocked()
}

// release returns a buffer to the pool.
func (l *chunkLease) release(buf []byte) {
	p := l.pool

	p.mu.Lock()
	defer p.mu.Unlock()

	l.held--
	p.held--
	p.free = append(p.free, buf)
}

// consumed records that one chunk has been delivered, from a buffer or a live
// stream, narrowing this reader's share in favour of its peers.
func (l *chunkLease) consumed() {
	p := l.pool

	p.mu.Lock()
	defer p.mu.Unlock()

	l.remaining--
	p.remaining--
}

// close drops the reader's claim. All acquired buffers must be released first.
func (l *chunkLease) close() {
	p := l.pool

	p.mu.Lock()
	defer p.mu.Unlock()

	if l.closed {
		return
	}
	l.closed = true
	p.remaining -= l.remaining
	l.remaining = 0
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
// A chunk that has no buffer when the consumer reaches it is streamed live from
// a GET of its own rather than waited for. Chunk 0 is always served this way,
// straight from the response that opened the download: it is the head of the
// line by definition, so there is nothing to reorder and nothing to buffer.
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

	// Live chunk state: the chunk being streamed straight from a GET rather
	// than a pool buffer. Only touched by the reading goroutine, as is err
	// below. liveIdx is -1 while no chunk is live.
	live    io.ReadCloser
	liveIdx int64
	liveLen int64 // bytes in the live chunk
	livePos int64 // bytes of the live chunk delivered so far
	reopens int   // connections opened for the live chunk; see downloadPartAttempts

	mu        sync.Mutex
	pending   map[int64]*downloadChunk
	nextFetch int64 // next chunk to request from the pool
	nextRead  int64 // next chunk to deliver to the consumer
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
		liveIdx:   -1,
		pending:   make(map[int64]*downloadChunk),
		nextFetch: 1,
	}
	r.startLive(0, live)

	r.mu.Lock()
	r.schedule()
	r.mu.Unlock()

	return r
}

// chunkLen returns the number of bytes in chunk idx; only the last chunk can be
// short.
func (r *multipartReader) chunkLen(idx int64) int64 {
	if start := idx * r.partSize; start+r.partSize > r.total {
		return r.total - start
	}
	return r.partSize
}

// startLive makes idx the live chunk, served from rc when one is already open
// and otherwise from a connection opened on the first read.
func (r *multipartReader) startLive(idx int64, rc io.ReadCloser) {
	r.live = rc
	r.liveIdx = idx
	r.liveLen = r.chunkLen(idx)
	r.livePos = 0
	r.reopens = 0
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
	length := r.chunkLen(ch.idx)

	var off int64
	for attempt := 0; ; attempt++ {
		if attempt > 0 && !r.backoff(attempt) {
			ch.err = fmt.Errorf("s3: download part %d of %s at offset %d: %w", ch.idx, r.key, start+off, r.ctx.Err())
			return
		}

		// Each attempt resumes from the last byte received, so a chunk that
		// keeps progressing across disconnects still completes.
		n, err := r.c.readRange(r.ctx, r.key, start+off, length-off, ch.buf[off:length])
		off += n
		if off >= length {
			ch.n = int(length)
			return
		}
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		if !r.retryable(err) || attempt+1 >= downloadPartAttempts {
			ch.err = fmt.Errorf("s3: download part %d of %s at offset %d: %w", ch.idx, r.key, start+off, err)
			return
		}
	}
}

// backoff waits out the delay for the given attempt, reporting false if the
// download was cancelled first.
func (r *multipartReader) backoff(attempt int) bool {
	t := time.NewTimer(downloadPartBackoff << (attempt - 1))
	defer t.Stop()

	select {
	case <-r.ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// retryable reports whether another attempt at the same offset could succeed.
// A cancelled context or a missing object never recovers; anything else is
// treated as a transient stream failure.
func (r *multipartReader) retryable(err error) bool {
	return r.ctx.Err() == nil && !errors.Is(err, os.ErrNotExist)
}

func (r *multipartReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	for {
		r.mu.Lock()

		// Close is terminal: no stream is reopened afterwards and every later
		// read reports it, rather than surfacing the cancelled context as a
		// fetch error.
		if r.closed {
			r.mu.Unlock()
			return 0, os.ErrClosed
		}
		if r.err != nil {
			r.mu.Unlock()
			return 0, r.err
		}

		// Nothing in hand: line up the next chunk, from its buffer if it has
		// one and live otherwise.
		if r.cur == nil && r.liveIdx < 0 {
			if r.nextRead > r.lastIdx {
				r.mu.Unlock()
				return 0, io.EOF
			}
			if ch := r.pending[r.nextRead]; ch != nil {
				r.mu.Unlock()
				<-ch.done
				r.mu.Lock()

				if r.closed {
					r.mu.Unlock()
					return 0, os.ErrClosed
				}
				if ch.err != nil {
					r.err = ch.err
					r.mu.Unlock()
					return 0, r.err
				}
				r.cur, r.curOff = ch, 0
			} else {
				// The pool had nothing to spare when this chunk came up.
				// Stream it rather than wait: a live chunk needs no buffer, so
				// the reader can never be held up by its peers. Chunks are
				// requested in order, so nothing past this one is in flight
				// either; look-ahead resumes from the chunk after it.
				r.startLive(r.nextRead, nil)
				r.nextFetch = r.nextRead + 1
				r.schedule()
			}
		}

		if r.liveIdx >= 0 {
			r.mu.Unlock()
			n, err := r.readLive(p)
			if n > 0 || err != nil {
				return n, err
			}

			// The live chunk is fully delivered; hand over to the next one.
			r.mu.Lock()
			r.liveIdx = -1
			r.nextRead++
			r.lease.consumed()
			r.schedule()
			r.mu.Unlock()
			continue
		}

		if r.curOff < r.cur.n {
			n := copy(p, r.cur.buf[r.curOff:r.cur.n])
			r.curOff += n
			if r.curOff >= r.cur.n {
				r.retire()
			}
			r.mu.Unlock()
			return n, nil
		}
		r.retire()
		r.mu.Unlock()
	}
}

// retire returns the current chunk's buffer to the pool and refills the window.
// r.mu must be held.
func (r *multipartReader) retire() {
	delete(r.pending, r.cur.idx)
	r.lease.release(r.cur.buf)
	r.lease.consumed()
	r.cur, r.curOff = nil, 0
	r.nextRead++
	r.schedule()
}

// liveOffset is the absolute object offset of the next live byte.
func (r *multipartReader) liveOffset() int64 {
	return r.base + r.liveIdx*r.partSize + r.livePos
}

// readLive serves the live chunk from its response body, reconnecting from the
// current offset if the stream breaks. Returning (0, nil) means the chunk has
// been delivered in full.
//
// reopens counts connections opened for this chunk over its lifetime, not
// consecutive failures, for the reason given on downloadPartAttempts. It is a
// field rather than a local because a read returning bytes returns from the
// function, and the budget must survive into the next call.
func (r *multipartReader) readLive(p []byte) (int, error) {
	fail := func(err error) error {
		r.err = fmt.Errorf("s3: download part %d of %s at offset %d: %w", r.liveIdx, r.key, r.liveOffset(), err)
		return r.err
	}

	for {
		remain := r.liveLen - r.livePos
		if remain <= 0 {
			return 0, nil
		}
		if int64(len(p)) > remain {
			p = p[:remain]
		}

		if r.live == nil {
			// The budget gates opening a connection, which is what it bounds.
			// Checking it after a read instead would let a stream that dribbles
			// a byte per connection return before the check is ever reached.
			if r.reopens >= downloadPartAttempts {
				return 0, fail(fmt.Errorf("%w after %d connections", io.ErrUnexpectedEOF, r.reopens))
			}
			if r.reopens > 0 && !r.backoff(r.reopens) {
				return 0, fail(r.ctx.Err())
			}
			r.reopens++

			rc, err := r.c.getRange(r.ctx, r.key, r.liveOffset(), remain)
			if err != nil {
				if !r.retryable(err) {
					return 0, fail(err)
				}
				continue
			}
			r.live = rc
		}

		n, err := r.live.Read(p)
		r.livePos += int64(n)
		if err != nil || r.livePos >= r.liveLen {
			// Either the chunk is complete or the stream broke; drop it so the
			// next attempt reopens from the current offset.
			r.closeLive()
		}

		if n > 0 {
			return n, nil
		}
		switch {
		case err == nil:
			continue // a legal empty read, not a failure
		case err == io.EOF && r.livePos >= r.liveLen:
			return 0, nil // chunk fully delivered
		}
		if !r.retryable(err) {
			return 0, fail(err)
		}
	}
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
