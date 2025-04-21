package s3

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/benbjohnson/litestream/internal"
)

func Download(ctx context.Context, sd *s3manager.Downloader, input *s3.GetObjectInput) io.ReadCloser {
	r, w := io.Pipe()
	m := NewPartManager(w, sd.PartSize, sd.Concurrency)

	go m.PipeCompletedParts()

	go func() {
		if _, err := sd.DownloadWithContext(ctx, m, input); err != nil {
			if isNotExists(err) {
				err = os.ErrNotExist
			}
			m.w.CloseWithError(err)
		} else {
			m.DownloadDone()
		}
	}()

	return r
}

type part struct {
	start   int64
	written int64
	buf     *bytes.Buffer
}

// The partManager manages the multi-part download done by the s3.Downloader,
// implementing the WriterAt interface by collecting data from downloading
// parts in a fixed number of Buffers, and piping them to the PipeWriter in
// the correct order.
//
// The manager manages max `concurrency` parts of `partSize` bytes. The parts
// begin in the `available` pool with assigned starting positions, moving to
// the `downloading` pool when data for the part is received by the Downloader.
// When fully written, the parts are transferred to the `done` pool from which
// they are piped, in order, to the PipeWriter and returned `available` pool to
// be used for the next part in line.
type partManager struct {
	w        *io.PipeWriter
	partSize int64

	freed      *sync.Cond      // conditionalized/signaled access to available
	nextInLine int64           // tracks the next position allowed to receive data
	available  map[int64]*part // parts available to receive data

	downloading sync.Map   // parts receiving data
	full        chan *part // transfers full parts from downloading to done

	done      sync.Map // parts ready to be flushed
	flushHead int64    // tracks the next position to be flushed
}

// Exposed for testing
func NewPartManager(w *io.PipeWriter, partSize int64, concurrency int) *partManager {
	m := &partManager{
		w:         w,
		partSize:  partSize,
		freed:     sync.NewCond(&sync.Mutex{}),
		available: make(map[int64]*part),
		full:      make(chan *part, concurrency),
	}
	for i := 0; i < concurrency; i++ {
		m.setFree(&part{})
	}
	return m
}

// A naive, first-come-first-serve free pool would be subject to head-of-line
// blocking. For example, in a pathological case, if the first part is slow to
// start receiving data, a subsequent part may complete its download and start
// receiving data for its next part, occupying all `n` buffers before the first
// part has a chance to reserve a buffer, thereby preventing the entire
// download from progressing (since parts have to be piped in order).
//
// To avoid this, free parts are explicitly reserved for the next parts in
// line, ensuring that the head of the line will always be able to receive a
// buffer and keep the pipe flowing.
func (m *partManager) setFree(p *part) {
	m.freed.L.Lock()
	defer m.freed.L.Unlock()

	m.available[m.nextInLine] = p
	m.nextInLine += m.partSize
	m.freed.Broadcast()
}

func (m *partManager) getFree(chunkStart int64) *part {
	m.freed.L.Lock()
	defer m.freed.L.Unlock()
	for {
		if p, exists := m.available[chunkStart]; exists {
			delete(m.available, chunkStart)
			return p
		}
		m.freed.Wait()
	}
}

// WriteAt is the interface called to receive data for chunks being downloaded
// by the s3.Downloader.
func (m *partManager) WriteAt(p []byte, pos int64) (n int, err error) {
	part := m.getDownloading(pos)
	n, err = part.buf.Write(p)
	if err == nil {
		part.written += int64(n)
		if part.written >= m.partSize {
			m.full <- part
		}
	}
	return
}

func (m *partManager) getDownloading(pos int64) *part {
	start := pos - (pos % m.partSize)
	if p, exists := m.downloading.Load(start); exists {
		return p.(*part)
	}

	part := m.getFree(start)
	part.start = start
	part.written = 0

	if part.buf == nil {
		// Pre-allocate space for the part, both for speed and to avoid
		// over-allocating with on-demand Buffer growth.
		part.buf = new(bytes.Buffer)
		part.buf.Grow(int(m.partSize))
	} else {
		part.buf.Reset()
	}

	m.downloading.Store(start, part)
	return part
}

// Exposed for testing
func (m *partManager) PipeCompletedParts() {
	for part := range m.full {
		// Move the part from downloading to done
		m.downloading.Delete(part.start)
		m.done.Store(part.start, part)

		// Flush the head of the line
		m.flush(&m.done)
	}

	// Once m.full is closed, there are no more writers.
	// The final part at the flushHead will be in m.downloading
	// if it did not reach the full partSize.
	m.flush(&m.downloading)
	m.w.Close()
}

// Exposed for testing
func (m *partManager) DownloadDone() {
	close(m.full)
}

func (m *partManager) flush(parts *sync.Map) {
	for p, exists := parts.Load(m.flushHead); exists; p, exists = parts.Load(m.flushHead) {
		p := p.(*part)
		io.Copy(m.w, p.buf)

		num := p.written
		m.setFree(p)

		parts.Delete(m.flushHead)
		m.flushHead += m.partSize

		// Each part corresponds to an underlying GET in the s3 client.
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()
		internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET").Add(float64(aws.Int64Value(&num)))
	}
}
