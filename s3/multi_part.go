package s3

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/benbjohnson/litestream/internal"
)

func Download(ctx context.Context, sd *s3manager.Downloader, input *s3.GetObjectInput) io.ReadCloser {
	r, w := io.Pipe()
	m := NewPartManager(w, sd.PartSize, sd.Concurrency)

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

	go m.PipeCompletedParts()

	return r
}

type part struct {
	start   int64
	written int64
	buf     *bytes.Buffer
}

// The partManager manages the multi-part download done by the s3.Downloader,
// implementing the WriterAt interface to buffer data from downloading parts
// and pipe them to PipeWriter in the correct order.
//
// The manager manages max `concurrency` parts of `partSize` bytes. The parts
// begin in the `free` pool with assigned starting positions, moving to the
// `downloading` pool when writes to the part are requested by the Downloader.
// When parts is fully written they are transferred to the `done` pool where
// they are piped, in order, to the PipeWriter and returned `free` pool to be
// reused for the next part in line.
//
// Note that it is important that the `free` pool specifically reserve buffers
// for the next parts in the download sequence; using an indiscriminate
// sync.Pool, for example, could otherwise result in head-of-line blocking, e.g.
// if the head-of-line iss slow to write its first bytes and all buffers are
// taken by later parts, essentially resulting in a deadlock.
type partManager struct {
	w        *io.PipeWriter
	partSize int64

	nextInLine atomic.Int64
	free       sync.Map // maps startPos -> chan *part that receives its reserved buffer

	downloading sync.Map   // maps startPos -> parts receiving data
	full        chan *part // transfers full parts from downloading to done

	flushHead int64
	done      sync.Map // maps startPos -> parts ready to be flushed
}

// Exposed for testing
func NewPartManager(w *io.PipeWriter, partSize int64, concurrency int) *partManager {
	m := &partManager{
		w:        w,
		partSize: partSize,
		full:     make(chan *part, concurrency),
	}
	for i := 0; i < concurrency; i++ {
		m.setFree(&part{}, make(chan *part, 1))
	}
	return m
}

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

func (m *partManager) getFree(chunkStart int64) *part {
	ch, exists := m.free.Load(chunkStart)
	if !exists {
		// Although not the common case, it is possible for the download of a
		// part to kick in before a buffer has been freed for it. In this case
		// a new channel is created to receive it (rather than reusing the channel
		// passed to setFree()).
		ch, _ = m.free.LoadOrStore(chunkStart, make(chan *part, 1))
	}
	return <-ch.(chan *part)
}

func (m *partManager) setFree(c *part, reuseChan chan *part) {
	nextStart := m.nextInLine.Add(m.partSize) - m.partSize
	ch, _ := m.free.LoadOrStore(nextStart, reuseChan)
	ch.(chan *part) <- c
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
	// The final part at the flushHead will be in downloading
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
		parts.Delete(m.flushHead)
		num := p.written

		// Reuse the channel for this part for the next free handoff
		ch, _ := m.free.Load(p.start)
		m.setFree(p, ch.(chan *part))

		m.flushHead += m.partSize

		// Each part corresponds to an underlying GET in the s3 client.
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()
		internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET").Add(float64(aws.Int64Value(&num)))
	}
}
