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
	m := NewChunkManager(w, sd.PartSize, sd.Concurrency)

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

	go m.PipeCompletedChunks()

	return r
}

type chunk struct {
	start   int64
	written int64
	buf     *bytes.Buffer
}

type chunkManager struct {
	w        *io.PipeWriter
	partSize int64

	free         []*chunk   // free pool of buffers
	nextInLine   *sync.Cond // next in line to start downloading
	downloadHead int64
	downloading  sync.Map // maps startPos -> chunks receiving data

	flushHead int64
	done      sync.Map // maps startPos -> chunks ready to be flushed

	full chan *chunk // full chunks
}

// Exposed for testing
func NewChunkManager(w *io.PipeWriter, partSize int64, concurrency int) *chunkManager {
	m := &chunkManager{
		w:          w,
		partSize:   partSize,
		free:       make([]*chunk, 0, concurrency),
		nextInLine: sync.NewCond(&sync.Mutex{}),
		full:       make(chan *chunk),
	}
	for i := 0; i < concurrency; i++ {
		m.free = append(m.free, &chunk{})
	}
	return m
}

func (m *chunkManager) WriteAt(p []byte, pos int64) (n int, err error) {
	chunk := m.getChunk(pos)
	n, err = chunk.buf.Write(p)
	if err == nil {
		chunk.written += int64(n)
		if chunk.written >= m.partSize {
			m.full <- chunk
		}
	}
	return
}

func (m *chunkManager) getChunk(pos int64) *chunk {
	chunkStart := pos - (pos % m.partSize)
	if c, exists := m.downloading.Load(chunkStart); exists {
		return c.(*chunk)
	}

	newChunk := m.getFree(chunkStart)
	newChunk.start = chunkStart
	newChunk.written = 0

	if newChunk.buf == nil {
		// Pre-allocate space for the chunk, both for speed and to avoid
		// over-allocating with on-demand Buffer growth.
		newChunk.buf = new(bytes.Buffer)
		newChunk.buf.Grow(int(m.partSize))
	} else {
		newChunk.buf.Reset()
	}

	m.downloading.Store(chunkStart, newChunk)
	return newChunk
}

func (m *chunkManager) getFree(chunkStart int64) *chunk {
	// To prevent head-of-line blocking, concurrent requesters take from
	// the free pool in head-of-line order.
	m.nextInLine.L.Lock()
	defer m.nextInLine.L.Unlock()

	for m.downloadHead != chunkStart || len(m.free) == 0 {
		m.nextInLine.Wait()
	}
	num := len(m.free)
	chunk := m.free[num-1]
	m.free = m.free[:num-1]
	m.downloadHead += m.partSize

	m.nextInLine.Broadcast()
	return chunk
}

func (m *chunkManager) setFree(c *chunk) {
	m.nextInLine.L.Lock()
	defer m.nextInLine.L.Unlock()

	m.free = append(m.free, c)
	m.nextInLine.Broadcast()
}

// Exposed for testing
func (m *chunkManager) PipeCompletedChunks() {
	for chunk := range m.full {
		// Move the chunk from m.pending to m.done
		m.downloading.Delete(chunk.start)
		m.done.Store(chunk.start, chunk)

		// Flush the head of the line
		m.flush(&m.done)
	}

	// Once m.full is closed, there are no more writers.
	// The final chunk at the flushHead will be in m.pending
	// if it did not reach the full partSize.
	m.flush(&m.downloading)
	m.w.Close()
}

// Exposed for testing
func (m *chunkManager) DownloadDone() {
	close(m.full)
}

func (m *chunkManager) flush(chunks *sync.Map) {
	for c, exists := chunks.Load(m.flushHead); exists; c, exists = chunks.Load(m.flushHead) {
		chunk := c.(*chunk)
		io.Copy(m.w, chunk.buf)
		chunks.Delete(m.flushHead)
		num := chunk.written

		m.setFree(chunk)

		m.flushHead += m.partSize

		// Each chunk corresponds to an underlying GET in the s3 client.
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()
		internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET").Add(float64(aws.Int64Value(&num)))
	}
}
