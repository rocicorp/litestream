package litestream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pierrec/lz4/v4"
)

func TestReplicaDownloadWALClosesSegmentsSequentially(t *testing.T) {
	const wideOffset = int64(0x11a6bfa58)
	client := &sequentialWALSegmentClient{segments: map[int64][]byte{
		0:          compressLZ4(t, []byte("first")),
		wideOffset: compressLZ4(t, []byte("second")),
	}}
	replica := NewReplica(nil, "")
	replica.Client = client
	dbPath := filepath.Join(t.TempDir(), "replica.db.tmp")

	if err := replica.downloadWAL(context.Background(), "generation", 0x12c3, []int64{0, wideOffset}, dbPath); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dbPath + "-000012c3-wal")
	if err != nil {
		t.Fatal(err)
	} else if want := []byte("firstsecond"); !bytes.Equal(got, want) {
		t.Fatalf("WAL=%q, want %q", got, want)
	}
}

type sequentialWALSegmentClient struct {
	ReplicaClient
	segments map[int64][]byte
	open     bool
}

func (c *sequentialWALSegmentClient) WALSegmentReader(_ context.Context, pos Pos) (io.ReadCloser, error) {
	if c.open {
		return nil, errors.New("opened WAL segment before closing previous segment")
	}
	c.open = true
	return &trackingReadCloser{
		Reader: bytes.NewReader(c.segments[pos.Offset]),
		close:  func() { c.open = false },
	}, nil
}

type trackingReadCloser struct {
	io.Reader
	close func()
}

func (r *trackingReadCloser) Close() error {
	r.close()
	return nil
}

func compressLZ4(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	} else if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
