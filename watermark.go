package litestream

import (
	"fmt"
	"io"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream/lite"
)

func (db *DB) readWatermarkFromPageMap(walFile *os.File, pageMap map[uint32]int64, includeDB bool) (string, error) {
	pos := db.WatermarkPos()
	if pos == nil {
		return "", nil
	}

	page := make([]byte, db.pageSize)
	if offset, ok := pageMap[pos.Page()]; ok {
		if n, err := walFile.ReadAt(page, offset+WALFrameHeaderSize); err != nil {
			return "", fmt.Errorf("read watermark page %d from wal: %w", pos.Page(), err)
		} else if n != len(page) {
			return "", fmt.Errorf("short read watermark page %d from wal", pos.Page())
		}
		return lite.ReadTextValueFromLeafPage(page, pos)
	}

	if !includeDB {
		return "", nil
	}

	if n, err := db.f.ReadAt(page, int64(pos.Page()-1)*int64(db.pageSize)); err != nil {
		return "", fmt.Errorf("read watermark page %d from database: %w", pos.Page(), err)
	} else if n != len(page) {
		return "", fmt.Errorf("short read watermark page %d from database", pos.Page())
	}
	return lite.ReadTextValueFromLeafPage(page, pos)
}

func (r *Replica) readWatermarkFromLTXFile(f *os.File) (string, error) {
	if r.db == nil || r.db.WatermarkPos() == nil {
		return "", nil
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("seek ltx file before watermark read: %w", err)
	}
	watermark, err := readWatermarkFromLTX(f, r.db.WatermarkPos())
	if err != nil {
		return "", err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("seek ltx file after watermark read: %w", err)
	}
	return watermark, nil
}

func readWatermarkFromLTX(r io.Reader, pos *lite.DBPos) (string, error) {
	dec := ltx.NewDecoder(r)
	if err := dec.DecodeHeader(); err != nil {
		return "", fmt.Errorf("decode ltx header: %w", err)
	}

	buf := make([]byte, dec.Header().PageSize)
	var watermark string
	for {
		var hdr ltx.PageHeader
		if err := dec.DecodePage(&hdr, buf); err == io.EOF {
			break
		} else if err != nil {
			return "", fmt.Errorf("decode ltx page: %w", err)
		}

		if hdr.Pgno != pos.Page() {
			continue
		}

		value, err := lite.ReadTextValueFromLeafPage(buf, pos)
		if err != nil {
			return "", fmt.Errorf("read watermark from ltx page %d: %w", pos.Page(), err)
		}
		watermark = value
	}

	if err := dec.Close(); err != nil {
		return "", fmt.Errorf("close ltx decoder: %w", err)
	}
	return watermark, nil
}

func (r *Replica) exportReplicaWatermark(watermark string) error {
	if watermark == "" || r.db == nil || r.Client == nil {
		return nil
	}

	labels := prometheus.Labels{
		"db":   r.db.Path(),
		"name": r.Name(),
	}
	replicaProgressGaugeVec.DeletePartialMatch(labels)

	labels["watermark"] = watermark
	gauge, err := replicaProgressGaugeVec.GetMetricWith(labels)
	if err != nil {
		return err
	}
	gauge.SetToCurrentTime()

	r.Logger().Info("replication watermark", "watermark", watermark)
	return nil
}

var replicaProgressGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "litestream_replica_progress",
	Help: "The last replicated watermark and time of replication.",
}, []string{"db", "name", "watermark"})

func init() {
	prometheus.MustRegister(replicaProgressGaugeVec)
}

// GatherReplicaMetrics returns replica metrics for tests and diagnostics.
func GatherReplicaMetrics() ([]*dto.MetricFamily, error) {
	return prometheus.DefaultGatherer.Gather()
}
