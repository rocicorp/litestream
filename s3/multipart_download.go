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
	"sync/atomic"
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

// Straggler handling.
//
// The consumer takes parts in order, so one slow connection idles every buffer
// behind it: the other parts finish, nothing new can be issued until the head
// part lands, and throughput collapses to that one connection for the tail of
// every window. Rather than wait, the reader splits the head part's remaining
// byte range and fetches the top half on a fresh connection, repeatedly, while
// there is proven idle capacity (parts finished but not yet consumed). Pieces
// write disjoint regions of one buffer, so no memory is added and no byte is
// fetched twice. A piece whose connection stops delivering bytes altogether is
// aborted so the normal retry path reopens it from where it stopped.
const (
	// hedgeMinIdle is how many finished-but-unconsumed parts must be queued
	// behind the head part before it is split. Each is a connection that has
	// gone idle, so a split only ever uses capacity already paid for.
	hedgeMinIdle = 2

	// hedgeMaxPieces bounds the connections a single part may fan out to.
	hedgeMaxPieces = 8
)

// Vars so tests can shorten them.
var (
	// hedgeCheckInterval is how often a consumer blocked on a part looks at it.
	hedgeCheckInterval = 250 * time.Millisecond

	// hedgeMinAge is how long a part must have been in flight before it is
	// split; younger parts are simply not done yet.
	hedgeMinAge = 2 * time.Second

	// hedgeMinPiece is the smallest remaining range worth a new connection.
	hedgeMinPiece = int64(1 << 20)

	// pieceReadStep is how much a piece reads per call. A split may not cut
	// below the end of the read in flight, so this bounds split granularity.
	pieceReadStep = int64(512 << 10)

	// stallTimeout is how long a piece may go without a byte before its
	// connection is aborted and reopened.
	stallTimeout = 15 * time.Second

	// connectStagger is the minimum spacing between connection opens by one
	// reader. Opening the whole pool in the same millisecond gets a large
	// fraction of the connections reset by S3 (observed: ~30 of 48), which
	// the SDK then retries a second later; spacing them out avoids the
	// resets and the lost second.
	connectStagger = 10 * time.Millisecond
)

// downloadPiece is one connection's share of a chunk: the byte range
// [from, to) of the chunk buffer. to can be lowered by a split, never below
// the end of the read in progress.
type downloadPiece struct {
	mu          sync.Mutex
	from        int64
	off         int64 // next byte to receive
	to          int64
	inflightEnd int64 // end of the read in progress
	lastByte    time.Time
	cancel      context.CancelFunc // aborts the attempt in progress
}

// downloadChunk is one part, fetched by one or more pieces, in flight or
// waiting to be consumed.
type downloadChunk struct {
	idx    int64
	buf    []byte
	n      int
	err    error
	done   chan struct{} // closed once every piece has finished
	doneAt time.Time     // when the fetch finished; for consumer-lag accounting
	began  time.Time

	mu       sync.Mutex
	pieces   []*downloadPiece // disjoint, covering [0, len) between them
	active   int              // pieces still running
	attempts int              // connections opened across all pieces
	retries  int              // reopens after a failure, across all pieces
}

// reserveRetry claims one of the chunk's downloadPartAttempts-1 retries,
// reporting false when the budget is spent. The budget is per chunk, not per
// piece, so splitting a part never multiplies the connections a broken range
// may open; a split's own first connection is bounded by hedgeMaxPieces.
func (ch *downloadChunk) reserveRetry() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.retries+1 >= downloadPartAttempts {
		return false
	}
	ch.retries++
	return true
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

	// nextOpen is the earliest time the next connection may be opened; see
	// connectStagger. Guarded by openMu since every piece goroutine consults it.
	openMu   sync.Mutex
	nextOpen time.Time

	// Diagnostics, reported at debug level. partRetries is written by fetch
	// goroutines; the rest only by the reading goroutine.
	started     time.Time
	bytesOut    int64
	partRetries atomic.Int64
	liveChunks  int64
	netWait     time.Duration // time Read spent blocked on a part still in flight
	netWaits    int64         // number of such waits
	consumerLag time.Duration // sum over parts of (consumed - downloaded)
	maxLag      time.Duration
	splits      int64        // pieces spawned by straggler splitting
	stallAborts int64        // connections aborted for delivering nothing
	ready       atomic.Int64 // parts finished but not yet consumed

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
		started:   time.Now(),
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
		ch := &downloadChunk{idx: r.nextFetch, buf: buf, done: make(chan struct{}), began: time.Now()}
		r.pending[ch.idx] = ch
		r.nextFetch++
		ch.mu.Lock()
		r.startPieceLocked(ch, 0, r.chunkLen(ch.idx))
		ch.mu.Unlock()
	}
}

// startPieceLocked starts a piece fetching [from, to) of ch. ch.mu must be held.
func (r *multipartReader) startPieceLocked(ch *downloadChunk, from, to int64) {
	p := &downloadPiece{from: from, off: from, to: to, lastByte: time.Now()}
	ch.pieces = append(ch.pieces, p)
	ch.active++
	r.wg.Add(1)
	go r.fetch(ch, p)
}

// fetch downloads one piece of a chunk into its region of the buffer,
// retrying in place so a transient failure does not discard the progress of
// the whole download. The last piece to finish completes the chunk.
func (r *multipartReader) fetch(ch *downloadChunk, p *downloadPiece) {
	defer r.wg.Done()
	defer r.finishPiece(ch)

	start := r.base + ch.idx*r.partSize
	for attempt := 0; ; attempt++ {
		if attempt > 0 && !r.backoff(attempt) {
			r.failPiece(ch, p, start, r.ctx.Err())
			return
		}

		p.mu.Lock()
		off, to := p.off, p.to
		p.mu.Unlock()
		if off >= to {
			return // a split took everything that was left
		}

		ch.mu.Lock()
		ch.attempts++
		ch.mu.Unlock()

		// Each attempt resumes from the last byte received, so a piece that
		// keeps progressing across disconnects still completes. The attempt
		// has its own context so a stalled read can be aborted without
		// cancelling its siblings.
		if !r.waitConnectSlot() {
			r.failPiece(ch, p, start, r.ctx.Err())
			return
		}
		ctx, cancel := context.WithCancel(r.ctx)
		p.mu.Lock()
		p.cancel = cancel
		p.mu.Unlock()
		rc, err := r.c.getRange(ctx, r.key, start+off, to-off)
		if err == nil {
			err = r.readPiece(rc, ch, p)
			_ = rc.Close()
		}
		p.mu.Lock()
		p.cancel = nil
		p.mu.Unlock()
		cancel()
		if err == nil {
			return
		}

		if !r.retryable(err) || !ch.reserveRetry() {
			r.failPiece(ch, p, start, err)
			return
		}
		r.partRetries.Add(1)
		p.mu.Lock()
		off = p.off
		p.mu.Unlock()
		r.c.logger.Debug("download part failed, retrying",
			"part", ch.idx, "piece", p.from, "offset", start+off, "received", off-p.from, "attempt", attempt+1, "error", err)
	}
}

// waitConnectSlot claims the next connection-open slot and sleeps until it
// arrives, reporting false if the download was cancelled first.
func (r *multipartReader) waitConnectSlot() bool {
	if connectStagger <= 0 {
		return true
	}
	r.openMu.Lock()
	now := time.Now()
	at := r.nextOpen
	if at.Before(now) {
		at = now
	}
	r.nextOpen = at.Add(connectStagger)
	r.openMu.Unlock()

	wait := time.Until(at)
	if wait <= 0 {
		return true
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-r.ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// readPiece copies rc into the piece's range one underlying read at a time,
// each capped at pieceReadStep, so a split can cut the range short at a step
// boundary and the stall check sees every byte as it lands rather than once
// per step. Returns nil once the piece has all its bytes.
func (r *multipartReader) readPiece(rc io.Reader, ch *downloadChunk, p *downloadPiece) error {
	for {
		p.mu.Lock()
		if p.off >= p.to {
			p.mu.Unlock()
			return nil
		}
		off := p.off
		end := min(off+pieceReadStep, p.to)
		p.inflightEnd = end
		p.mu.Unlock()

		n, err := rc.Read(ch.buf[off:end])

		p.mu.Lock()
		p.off += int64(n)
		if n > 0 {
			p.lastByte = time.Now()
		}
		done := p.off >= p.to
		p.mu.Unlock()

		if err != nil {
			if done && err == io.EOF {
				return nil
			}
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
	}
}

// failPiece records the first error for the chunk. Sibling pieces run to
// completion regardless, since the buffer cannot go back to the pool while
// any of them might still write into it.
func (r *multipartReader) failPiece(ch *downloadChunk, p *downloadPiece, start int64, err error) {
	p.mu.Lock()
	off := p.off
	p.mu.Unlock()
	ch.mu.Lock()
	if ch.err == nil {
		ch.err = fmt.Errorf("s3: download part %d of %s at offset %d: %w", ch.idx, r.key, start+off, err)
	}
	ch.mu.Unlock()
}

// finishPiece retires one piece and, when it was the last, completes the chunk.
func (r *multipartReader) finishPiece(ch *downloadChunk) {
	ch.mu.Lock()
	ch.active--
	last := ch.active == 0
	pieces, attempts, err := len(ch.pieces), ch.attempts, ch.err
	ch.mu.Unlock()
	if !last {
		return
	}
	length := r.chunkLen(ch.idx)
	if err == nil {
		ch.n = int(length)
		elapsed := time.Since(ch.began)
		r.c.logger.Debug("download part done",
			"part", ch.idx, "offset", r.base+ch.idx*r.partSize, "bytes", length,
			"pieces", pieces, "attempts", attempts,
			"elapsed", elapsed.Round(time.Millisecond), "mbps", mbps(length, elapsed))
	}
	ch.doneAt = time.Now()
	r.ready.Add(1)
	close(ch.done)
}

// awaitChunk blocks until ch is complete, checking it periodically for
// stragglers while it waits.
func (r *multipartReader) awaitChunk(ch *downloadChunk) {
	t := time.Now()
	defer func() {
		r.netWait += time.Since(t)
		r.netWaits++
	}()

	select {
	case <-ch.done:
		return
	default:
	}
	tick := time.NewTicker(hedgeCheckInterval)
	defer tick.Stop()
	for {
		select {
		case <-ch.done:
			return
		case <-tick.C:
			r.hedge(ch)
		}
	}
}

// hedge is run for the head-of-line chunk while the consumer is blocked on
// it. It aborts any piece whose connection has delivered nothing for
// stallTimeout, and, when idle capacity has built up behind the chunk, hands
// the top half of the largest outstanding range to a new connection.
func (r *multipartReader) hedge(ch *downloadChunk) {
	now := time.Now()
	ch.mu.Lock()
	defer ch.mu.Unlock()

	for _, p := range ch.pieces {
		p.mu.Lock()
		if p.cancel != nil && p.off < p.to && now.Sub(p.lastByte) > stallTimeout {
			p.cancel()
			p.lastByte = now // one abort per stall
			r.stallAborts++
			r.c.logger.Debug("download part stalled, reconnecting",
				"part", ch.idx, "piece", p.from, "offset", p.off, "remaining", p.to-p.off)
		}
		p.mu.Unlock()
	}

	// Each hedge connection is backed by one idle connection: a part that has
	// finished but not been consumed. Pieces still running beyond the primary
	// hold their slot until they finish, so a chunk cannot fan out past the
	// capacity that actually went idle.
	idle := r.ready.Load()
	if ch.active == 0 || len(ch.pieces) >= hedgeMaxPieces ||
		now.Sub(ch.began) < hedgeMinAge || idle < hedgeMinIdle || int64(ch.active-1) >= idle {
		return
	}

	// Split the piece with the most bytes still to come, at the end of the
	// read it has in flight so the two never overlap.
	var best *downloadPiece
	var bestCut, bestRem int64
	for _, p := range ch.pieces {
		p.mu.Lock()
		cut := max(p.inflightEnd, p.off)
		rem := p.to - cut
		p.mu.Unlock()
		if rem > bestRem {
			best, bestCut, bestRem = p, cut, rem
		}
	}
	if best == nil || bestRem < 2*hedgeMinPiece {
		return
	}
	mid := bestCut + bestRem/2
	best.mu.Lock()
	// Re-read under the piece lock: the read in flight may have advanced.
	if cut := max(best.inflightEnd, best.off); cut > bestCut {
		bestCut = cut
		mid = bestCut + (best.to-bestCut)/2
	}
	if best.to-mid < hedgeMinPiece || mid-bestCut < hedgeMinPiece {
		best.mu.Unlock()
		return
	}
	oldTo := best.to
	best.to = mid
	best.mu.Unlock()

	r.startPieceLocked(ch, mid, oldTo)
	r.splits++
	r.c.logger.Debug("download part split",
		"part", ch.idx, "at", mid, "remaining", oldTo-mid, "pieces", len(ch.pieces),
		"idle", r.ready.Load(), "age", now.Sub(ch.began).Round(time.Millisecond))
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

	// The lock is never held across a wait. Close needs it to mark the reader
	// closed before it cancels the context, and that cancellation is what
	// unblocks a wait on the network.
	for {
		n, wait, err := r.advance(p)
		r.bytesOut += int64(n)
		if n > 0 || err != nil {
			return n, err
		}
		if wait != nil {
			r.awaitChunk(wait)
			continue
		}

		n, err = r.readLive(p)
		r.bytesOut += int64(n)
		if n > 0 || err != nil {
			return n, err
		}
		r.finishLive()
	}
}

// advance serves bytes from the chunk in hand, or lines up the next one when
// there is none. A pooled chunk still in flight is returned for the caller to
// wait on; a chunk that never got a buffer becomes the live chunk, which the
// caller serves itself. Returning (0, nil, nil) means the live chunk is up.
func (r *multipartReader) advance(p []byte) (int, *downloadChunk, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Close is terminal: no stream is reopened afterwards and every later read
	// reports it, rather than surfacing the cancelled context as a fetch error.
	if r.closed {
		return 0, nil, os.ErrClosed
	}
	if r.err != nil {
		return 0, nil, r.err
	}
	if r.liveIdx >= 0 {
		return 0, nil, nil
	}

	for {
		if r.cur == nil {
			if r.nextRead > r.lastIdx {
				return 0, nil, io.EOF
			}
			ch := r.pending[r.nextRead]
			if ch == nil {
				// The pool had nothing to spare when this chunk came up.
				// Stream it rather than wait: a live chunk needs no buffer, so
				// the reader can never be held up by its peers. Chunks are
				// requested in order, so nothing past this one is in flight
				// either; look-ahead resumes from the chunk after it.
				r.liveChunks++
				r.c.logger.Debug("no pool buffer for part, streaming live",
					"part", r.nextRead, "held", r.lease.held, "pending", len(r.pending))
				r.startLive(r.nextRead, nil)
				r.nextFetch = r.nextRead + 1
				r.schedule()
				return 0, nil, nil
			}
			select {
			case <-ch.done:
			default:
				return 0, ch, nil
			}
			if ch.err != nil {
				r.err = ch.err
				return 0, nil, r.err
			}
			r.cur, r.curOff = ch, 0
		}

		if r.curOff < r.cur.n {
			n := copy(p, r.cur.buf[r.curOff:r.cur.n])
			r.curOff += n
			if r.curOff >= r.cur.n {
				r.retire()
			}
			return n, nil, nil
		}
		r.retire()
	}
}

// finishLive hands over from a fully delivered live chunk to the next one.
func (r *multipartReader) finishLive() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.liveIdx = -1
	r.nextRead++
	r.lease.consumed()
	r.schedule()
	r.progressLocked()
}

// retire returns the current chunk's buffer to the pool and refills the window.
// r.mu must be held.
func (r *multipartReader) retire() {
	if lag := time.Since(r.cur.doneAt); lag > 0 {
		r.consumerLag += lag
		if lag > r.maxLag {
			r.maxLag = lag
		}
	}
	delete(r.pending, r.cur.idx)
	r.ready.Add(-1)
	r.lease.release(r.cur.buf)
	r.lease.consumed()
	r.cur, r.curOff = nil, 0
	r.nextRead++
	r.schedule()
	r.progressLocked()
}

// progressInterval is how many delivered parts separate progress log lines.
const progressInterval = 64

// progressLocked logs a progress line every progressInterval delivered parts.
// r.mu must be held.
func (r *multipartReader) progressLocked() {
	if r.nextRead%progressInterval != 0 {
		return
	}
	elapsed := time.Since(r.started)
	r.c.logger.Debug("download progress",
		"parts", r.nextRead, "of", r.lastIdx+1, "bytes", r.bytesOut, "total", r.total,
		"elapsed", elapsed.Round(time.Second), "mbps", mbps(r.bytesOut, elapsed),
		"window", r.lease.held, "in-flight", len(r.pending),
		"part-retries", r.partRetries.Load(), "live-chunks", r.liveChunks,
		"net-wait", r.netWait.Round(time.Millisecond), "net-waits", r.netWaits,
		"consumer-lag-avg", r.avgLag().Round(time.Millisecond), "consumer-lag-max", r.maxLag.Round(time.Millisecond),
		"splits", r.splits, "stall-aborts", r.stallAborts)
}

// avgLag is the mean time a downloaded part sat in its buffer before the
// consumer took it. Large values with little net-wait mean the consumer, not
// the network, is the bottleneck.
func (r *multipartReader) avgLag() time.Duration {
	if r.nextRead <= 1 {
		return 0
	}
	return r.consumerLag / time.Duration(r.nextRead-1)
}

// mbps formats a throughput in megabytes per second.
func mbps(n int64, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / d.Seconds() / (1024 * 1024)
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

			if r.reopens > 1 {
				r.c.logger.Debug("live part reconnecting",
					"part", r.liveIdx, "offset", r.liveOffset(), "remaining", remain, "connection", r.reopens)
			}
			rc, err := r.c.getRange(r.ctx, r.key, r.liveOffset(), remain)
			if err != nil {
				if !r.retryable(err) {
					return 0, fail(err)
				}
				r.c.logger.Debug("live part open failed",
					"part", r.liveIdx, "offset", r.liveOffset(), "connection", r.reopens, "error", err)
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
		r.c.logger.Debug("live part read failed",
			"part", r.liveIdx, "offset", r.liveOffset(), "connection", r.reopens, "error", err)
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

	elapsed := time.Since(r.started)
	r.c.logger.Debug("multipart download closed",
		"key", r.key, "bytes", r.bytesOut, "total", r.total, "parts", r.lastIdx+1,
		"elapsed", elapsed.Round(time.Millisecond), "mbps", mbps(r.bytesOut, elapsed),
		"part-retries", r.partRetries.Load(), "live-chunks", r.liveChunks,
		"net-wait", r.netWait.Round(time.Millisecond), "net-waits", r.netWaits,
		"consumer-lag-avg", r.avgLag().Round(time.Millisecond), "consumer-lag-max", r.maxLag.Round(time.Millisecond),
		"splits", r.splits, "stall-aborts", r.stallAborts, "err", r.err)

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
