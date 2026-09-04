//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestRestore_S3MultipartDownload verifies that restoring through the parallel
// multipart download path produces the same database as the single-stream path.
//
// The part size is forced well below the default so a modest test database still
// spans many parts, and the two restores are compared against each other as well
// as against the source.
func TestRestore_S3MultipartDownload(t *testing.T) {
	RequireBinaries(t)
	RequireDocker(t)

	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	containerName, endpoint := StartMinioTestContainer(t)
	t.Cleanup(func() { StopMinioTestContainer(t, containerName) })

	ctx := context.Background()
	s3Client := newMinioS3Client(t, endpoint, true)

	bucket := fmt.Sprintf("litestream-multipart-%d", time.Now().UnixNano())
	createBucket(t, ctx, s3Client, bucket)
	t.Cleanup(func() {
		if err := clearBucket(context.Background(), s3Client, bucket); err != nil {
			t.Logf("warn: clear bucket: %v", err)
		}
	})

	db := SetupTestDB(t, "s3-multipart-download")
	t.Cleanup(db.Cleanup)

	if err := db.Create(); err != nil {
		t.Fatalf("create db: %v", err)
	}
	if err := db.Populate("30MB"); err != nil {
		t.Fatalf("populate db: %v", err)
	}

	replicaURL := fmt.Sprintf("s3://%s/multipart", bucket)
	db.ReplicaURL = replicaURL

	replicateConfig := writeMultipartConfig(t, "replicate", db.Path, replicaURL, endpoint, "")
	if err := db.StartLitestreamWithConfig(replicateConfig); err != nil {
		t.Fatalf("start litestream: %v", err)
	}

	waitForObjects(t, s3Client, bucket, "multipart", 60*time.Second)
	time.Sleep(3 * time.Second)

	if err := db.StopLitestream(); err != nil {
		t.Fatalf("stop litestream: %v", err)
	}
	db.LitestreamCmd = nil

	// Parallel parts, small enough that the snapshot spans many of them.
	multipartPath := filepath.Join(db.TempDir, "restored-multipart.db")
	multipartOut := restoreWithConfig(t, db.Path, multipartPath,
		writeMultipartConfig(t, "multipart", db.Path, replicaURL, endpoint, `
        download-part-size: 1MiB
        download-concurrency: 8`))

	if !strings.Contains(multipartOut, "downloading in parallel parts") {
		t.Fatalf("expected the multipart download path to be used; restore output:\n%s", multipartOut)
	}

	// Same restore with multipart disabled.
	singlePath := filepath.Join(db.TempDir, "restored-single.db")
	singleOut := restoreWithConfig(t, db.Path, singlePath,
		writeMultipartConfig(t, "single", db.Path, replicaURL, endpoint, `
        download-concurrency: 0`))

	if strings.Contains(singleOut, "downloading in parallel parts") {
		t.Fatalf("multipart download used despite download-concurrency: 0; restore output:\n%s", singleOut)
	}

	for _, path := range []string{multipartPath, singlePath} {
		if err := compareRowCounts(db.Path, path); err != nil {
			t.Fatalf("row compare %s: %v", filepath.Base(path), err)
		}
	}

	multipartBytes, err := os.ReadFile(multipartPath)
	if err != nil {
		t.Fatal(err)
	}
	singleBytes, err := os.ReadFile(singlePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(multipartBytes) != len(singleBytes) {
		t.Fatalf("restored sizes differ: multipart=%d single=%d", len(multipartBytes), len(singleBytes))
	}
	for i := range multipartBytes {
		if multipartBytes[i] != singleBytes[i] {
			t.Fatalf("restored databases differ at byte %d", i)
		}
	}
}

// writeMultipartConfig writes a MinIO-backed config with extra replica options
// appended, and debug logging so the restore path is observable.
func writeMultipartConfig(t *testing.T, name, dbPath, replicaURL, endpoint, extra string) string {
	t.Helper()

	configPath := filepath.Join(filepath.Dir(dbPath), fmt.Sprintf("litestream-%s.yml", name))
	config := fmt.Sprintf(`access-key-id: minioadmin
secret-access-key: minioadmin

logging:
  level: debug

dbs:
  - path: %s
    snapshot:
      interval: 1s
      retention: 1h
    replicas:
      - url: %s
        endpoint: %s
        region: us-east-1
        force-path-style: true
        skip-verify: true
        sync-interval: 1s%s
`, filepath.ToSlash(dbPath), replicaURL, endpoint, extra)

	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return configPath
}

func restoreWithConfig(t *testing.T, dbPath, outputPath, configPath string) string {
	t.Helper()

	cmd := exec.Command(getBinaryPath("litestream"), "restore",
		"-config", configPath,
		"-o", outputPath,
		dbPath,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("restore failed: %v\nOutput: %s", err, out)
	}
	return string(out)
}
