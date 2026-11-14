package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/util/tmpfsfile"
	stores3 "github.com/kuasar-sandbox/accelerator/pkg/store/s3"
	storeserver "github.com/kuasar-sandbox/accelerator/pkg/store/server"
)

type chunkFileStats struct {
	storeserver.ChunkStats
	DedupRatePercent          *float64 `json:"dedup_rate_percent"`
	CacheInterceptRatePercent *float64 `json:"cache_intercept_rate_percent"`
	S3HeadCallsTotal          uint64   `json:"s3_head_calls_total"`
	S3PutCallsTotal           uint64   `json:"s3_put_calls_total"`
}

func (s chunkFileStats) add(b chunkFileStats) chunkFileStats {
	s.PutRequestsTotal += b.PutRequestsTotal
	s.DedupTotal += b.DedupTotal
	s.CacheInterceptedTotal += b.CacheInterceptedTotal
	s.S3HeadCallsTotal += b.S3HeadCallsTotal
	s.S3PutCallsTotal += b.S3PutCallsTotal
	return s
}

type fileSnapshot struct {
	SchemaVersion int                        `json:"schema_version"`
	StorageID     string                     `json:"storage_id"`
	CounterEpoch  string                     `json:"counter_epoch"`
	InstanceID    string                     `json:"instance_id"`
	UpdatedAt     time.Time                  `json:"updated_at"`
	ServiceState  string                     `json:"service_state"`
	UptimeSec     int64                      `json:"uptime_sec"`
	Backend       string                     `json:"backend"`
	Chunk         chunkFileStats             `json:"chunk"`
	MetadataCache stores3.MetadataCacheStats `json:"chunk_metadata_cache"`
}

type statsFile struct {
	path                              string
	lock                              *os.File
	interval                          time.Duration
	start                             time.Time
	epoch, instance, storage, backend string
	base                              fileSnapshot
	server                            *storeserver.Server
	s3                                *stores3.Store
	log                               *slog.Logger
	once                              sync.Once
}

func statsID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func newStatsFile(cfg Config, backend storeserver.Backend, server *storeserver.Server, log *slog.Logger) (*statsFile, error) {
	if cfg.StatsFile == nil || *cfg.StatsFile == "" {
		return nil, nil
	}
	name := *cfg.StatsFile
	if err := tmpfsfile.Dir(filepath.Dir(name)); err != nil {
		return nil, fmt.Errorf("store stats_file: %w", err)
	}
	lock, err := tmpfsfile.Lock(name + ".lock")
	if err != nil {
		return nil, err
	}
	interval, _ := time.ParseDuration(cfg.StatsFileInterval)
	f := &statsFile{path: name, lock: lock, interval: interval, start: time.Now(), epoch: statsID(), instance: statsID(), server: server, backend: cfg.Backend, log: log}
	f.s3, _ = backend.(*stores3.Store)
	if cfg.Backend == "s3" {
		f.storage = cfg.S3CacheNamespace()
	} else {
		f.storage = "fs:" + filepath.Clean(cfg.FS.Root)
	}
	old, err := os.Open(name)
	if err == nil {
		data, readErr := io.ReadAll(io.LimitReader(old, 65537))
		_ = old.Close()
		var base fileSnapshot
		decodeErr := json.Unmarshal(data, &base)
		if readErr == nil && len(data) <= 65536 && decodeErr == nil && base.SchemaVersion == 1 && base.StorageID == f.storage && base.CounterEpoch != "" {
			f.base = base
			f.epoch = base.CounterEpoch
		} else {
			log.Warn("stats_file history ignored: invalid format or different storage", "path", name)
		}
	} else if !os.IsNotExist(err) {
		_ = lock.Close()
		return nil, err
	}
	// Detect unwritable/full filesystems before accepting traffic. Later write
	// failures preserve the last complete file and do not fail data requests.
	if err = f.publish("starting"); err != nil {
		_ = lock.Close()
		return nil, err
	}
	return f, nil
}

func (f *statsFile) snapshot(state string) fileSnapshot {
	s := fileSnapshot{SchemaVersion: 1, StorageID: f.storage, CounterEpoch: f.epoch, InstanceID: f.instance, UpdatedAt: time.Now().UTC(), ServiceState: state, UptimeSec: int64(time.Since(f.start).Seconds()), Backend: f.backend}
	s.Chunk.ChunkStats = f.server.Stats().Chunk
	s.MetadataCache = stores3.MetadataCacheStats{State: "disabled"}
	if f.s3 != nil {
		s.Chunk.S3HeadCallsTotal, s.Chunk.S3PutCallsTotal = f.s3.ChunkIOStats()
		s.MetadataCache = f.s3.MetadataCacheStats()
	}
	s.Chunk = s.Chunk.add(f.base.Chunk)
	s.Chunk.DedupRatePercent = ratePercent(s.Chunk.DedupTotal, s.Chunk.PutRequestsTotal)
	s.Chunk.CacheInterceptRatePercent = ratePercent(s.Chunk.CacheInterceptedTotal, s.Chunk.PutRequestsTotal)
	s.MetadataCache.CacheCounters = s.MetadataCache.CacheCounters.Add(f.base.MetadataCache.CacheCounters)
	return s
}
func (f *statsFile) publish(state string) error {
	body, err := json.MarshalIndent(f.snapshot(state), "", "  ")
	if err != nil {
		return err
	}
	return tmpfsfile.Write(f.path, append(body, '\n'))
}
func (f *statsFile) run(ctx context.Context) {
	tick := time.NewTicker(f.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := f.publish("running"); err != nil {
				f.log.Warn("stats_file update failed; retaining previous snapshot", "error", err)
			}
		}
	}
}
func (f *statsFile) close() {
	f.once.Do(func() {
		if err := f.publish("stopped"); err != nil {
			f.log.Warn("stats_file final update failed", "error", err)
		}
		if err := f.lock.Close(); err != nil {
			f.log.Warn("stats_file close failed", "error", err)
		}
	})
}

// Rates are derived after restoring cumulative counters, never restored or averaged.
func ratePercent(part, total uint64) *float64 {
	if total == 0 {
		return nil
	}
	value := math.Round(float64(part)/float64(total)*10000) / 100
	return &value
}
