package s3_test

import (
	"bytes"
	crypto "crypto/rand"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/benbjohnson/litestream/s3"
)

func TestMultiPartDownload(t *testing.T) {
	const numBytes = 1234 * 1023 // Non-round size to ensure a partial part at the end
	const partSize = 1000

	for _, c := range []int{1, 2, 23} {
		t.Run(fmt.Sprintf("concurrency=%d", c), func(t *testing.T) {
			testMultiPartDownload(t, numBytes, partSize, c)
		})
	}
}

func testMultiPartDownload(t *testing.T, numBytes int64, partSize int64, concurrency int) {
	randomBytes := make([]byte, numBytes)
	if _, err := crypto.Read(randomBytes); err != nil {
		t.Fatal(err)
	}

	r, w := io.Pipe()
	m := s3.NewPartManager(w, partSize, concurrency)
	go m.PipeCompletedParts()

	var head atomic.Int64
	nextHead := func() int64 {
		return head.Add(partSize) - partSize
	}

	// Simulates the s3.Downloader by running `concurrency` routines that
	// take `partSize`` slices of the randomBytes from the head and call
	// m.WriteAt() to randomly send the slices to the part manager.
	var requests sync.WaitGroup
	requests.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			for start := nextHead(); start < numBytes; start = nextHead() {
				for end := min(numBytes, start+partSize); start < end; {
					len := rand.Int63n(end-start) + 1
					m.WriteAt(randomBytes[start:start+len], start)
					start += len
				}
			}
			requests.Done()
		}()
	}

	go func() {
		requests.Wait()
		m.DownloadDone()
	}()

	if result, err := io.ReadAll(r); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(randomBytes, result) {
		t.Fatalf("downloaded bytes not equal")
	}
}
