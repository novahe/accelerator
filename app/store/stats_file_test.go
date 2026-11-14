package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/accelerator/internal/util/tmpfsfile"
	pkgstore "github.com/kuasar-sandbox/accelerator/pkg/store"
	storeclient "github.com/kuasar-sandbox/accelerator/pkg/store/client"
	storeconfig "github.com/kuasar-sandbox/accelerator/pkg/store/config"
	storefs "github.com/kuasar-sandbox/accelerator/pkg/store/fs"
	storeserver "github.com/kuasar-sandbox/accelerator/pkg/store/server"
)

func statsTmpfs(t *testing.T) string {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("Linux tmpfs")
	}
	dir, err := os.MkdirTemp("/dev/shm", "store-app-metrics-")
	if err != nil {
		t.Skipf("tmpfs unavailable: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}
func stringPtr(v string) *string { return &v }
func readStatsSnapshot(t *testing.T, name string) fileSnapshot {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var s fileSnapshot
	if err = json.Unmarshal(b, &s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestStatsFileAtomicPublishRestoreAndStorageIsolation(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Listen: "127.0.0.1:0", Backend: "fs", FS: &storeconfig.FSConfig{Root: root}, StatsFile: stringPtr(filepath.Join(statsTmpfs(t), "auto", "stats.json")), StatsFileInterval: "1s"}
	backend, err := storefs.New(storefs.Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := storeserver.New(storeserver.Options{Backend: backend, Generations: func() []pkgstore.Generation { return []pkgstore.Generation{"G1"} }})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	f, err := newStatsFile(cfg, backend, srv, logger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = newStatsFile(cfg, backend, srv, logger); err == nil {
		t.Fatal("second publisher acquired lock")
	}
	// A small base makes persistence assertions independent of server internals.
	initial := readStatsSnapshot(t, f.path)
	if initial.Chunk.DedupRatePercent != nil || initial.Chunk.CacheInterceptRatePercent != nil {
		t.Fatal("empty totals must produce JSON null rates")
	}
	f.base.Chunk.PutRequestsTotal = 37
	f.base.Chunk.DedupTotal = 30
	f.base.Chunk.CacheInterceptedTotal = 29
	stale := 123.45
	f.base.Chunk.DedupRatePercent = &stale
	f.base.Chunk.CacheInterceptRatePercent = &stale
	f.base.MetadataCache.ErrorsTotal = 29
	f.base.MetadataCache.FillsDroppedTotal = 7
	readDone := make(chan struct{})
	readerErr := make(chan error, 1)
	go func() {
		defer close(readDone)
		for i := 0; i < 1000; i++ {
			b, err := os.ReadFile(f.path)
			if err != nil {
				readerErr <- err
				return
			}
			var v fileSnapshot
			if err = json.Unmarshal(b, &v); err != nil {
				readerErr <- err
				return
			}
		}
	}()
	for i := 0; i < 100; i++ {
		if err = f.publish("running"); err != nil {
			t.Fatal(err)
		}
	}
	<-readDone
	select {
	case err := <-readerErr:
		t.Fatal(err)
	default:
	}
	old := readStatsSnapshot(t, f.path)
	f.close()
	next, err := newStatsFile(cfg, backend, srv, logger)
	if err != nil {
		t.Fatal(err)
	}
	current := readStatsSnapshot(t, next.path)
	if old.CounterEpoch != current.CounterEpoch || old.InstanceID == current.InstanceID || current.Chunk.PutRequestsTotal != 37 || current.MetadataCache.ErrorsTotal != 29 || current.MetadataCache.Entries != 0 || current.MetadataCache.FillsDroppedTotal != 7 {
		t.Fatalf("restore: old=%+v new=%+v", old, current)
	}
	if current.Chunk.DedupRatePercent == nil || *current.Chunk.DedupRatePercent != 81.08 || current.Chunk.CacheInterceptRatePercent == nil || *current.Chunk.CacheInterceptRatePercent != 78.38 {
		t.Fatalf("restored rates not recomputed: %+v", current.Chunk)
	}
	next.close()
	cfg.FS.Root = t.TempDir()
	changed, err := newStatsFile(cfg, backend, srv, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer changed.close()
	reset := readStatsSnapshot(t, changed.path)
	if reset.CounterEpoch == old.CounterEpoch || reset.Chunk.PutRequestsTotal != 0 {
		t.Fatal("mixed counters across storage identities")
	}
}

func TestStatsFileBadHistoryAndRuntimeWriteFailure(t *testing.T) {
	cfg := Config{Backend: "fs", FS: &storeconfig.FSConfig{Root: t.TempDir()}, StatsFile: stringPtr(filepath.Join(statsTmpfs(t), "stats.json")), StatsFileInterval: "1s"}
	backend, err := storefs.New(storefs.Config{Root: cfg.FS.Root})
	if err != nil {
		t.Fatal(err)
	}
	srv, _ := storeserver.New(storeserver.Options{Backend: backend, Generations: func() []pkgstore.Generation { return []pkgstore.Generation{"G1"} }})
	if err = os.WriteFile(*cfg.StatsFile, []byte("broken JSON"), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := newStatsFile(cfg, backend, srv, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	original := f.path
	f.path = filepath.Join(filepath.Dir(original), "missing", "stats.json")
	if err = f.publish("running"); err == nil {
		t.Fatal("unwritable target accepted")
	}
	f.path = original
	if s := readStatsSnapshot(t, original); s.SchemaVersion != 1 || s.Chunk.PutRequestsTotal != 0 {
		t.Fatal("invalid history reused or previous snapshot damaged")
	}
	f.close()
}

// Exercise real gRPC -> App -> signed AWS SDK HTTP requests. This fixture
// checks wire counts; the standalone MinIO run supplies real S3 compatibility.
func TestRunS3MetadataReloadAndPersistentStats(t *testing.T) {
	var mu sync.Mutex
	objects := make(map[string][]byte)
	var heads, puts atomic.Int64
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			puts.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(500)
				return
			}
			objects[r.URL.Path] = body
			w.Header().Set("ETag", `"test-etag"`)
		case http.MethodHead:
			heads.Add(1)
			body, ok := objects[r.URL.Path]
			if !ok {
				w.WriteHeader(404)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		case http.MethodGet:
			body, ok := objects[r.URL.Path]
			if !ok {
				w.WriteHeader(404)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			_, _ = w.Write(body)
		default:
			w.WriteHeader(405)
		}
	}))
	defer remote.Close()
	dir := statsTmpfs(t)
	cfg := Config{Listen: runTestAddress(t), Backend: "s3", S3: &storeconfig.S3Config{Endpoint: remote.URL, Bucket: "test", AccessKey: "test-ak", SecretKey: "test-secret", ChunkMetadataCache: &storeconfig.ChunkMetadataCacheConfig{Dir: filepath.Join(dir, "cache")}}, StatsFile: stringPtr(filepath.Join(dir, "stats.json")), StatsFileInterval: "10ms", Generations: &storeconfig.GenerationsConfig{Config: []string{"G1"}}}
	data := []byte("RPC and cache survive Store restart")
	key := pkgstore.ContentKey(sha256.Sum256(data))
	put := func(addr string, expectedNew bool) {
		t.Helper()
		c, err := storeclient.New(addr, 1, runTestTimeout)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		ctx, stop := context.WithTimeout(context.Background(), runTestTimeout)
		defer stop()
		admission, err := c.AdmitWrite(ctx)
		if err != nil {
			t.Fatal(err)
		}
		fresh, err := c.Put(ctx, admission, pkgstore.PartitionChunk, key, data)
		if err != nil || fresh != expectedNew {
			t.Fatalf("put=%v %v want new=%v", fresh, err, expectedNew)
		}
		found, body, err := c.Get(ctx, pkgstore.PartitionChunk, key)
		if err != nil || !found || string(body) != string(data) {
			t.Fatalf("get=%v %q %v", found, body, err)
		}
	}
	cancel, done := startRunTest(t, cfg, Options{})
	conn := waitForRunTestStore(t, cfg.Listen)
	_ = conn.Close()
	put(cfg.Listen, true)
	for i := 0; i < 5; i++ {
		put(cfg.Listen, false)
	}
	finishRunTest(t, cancel, done, cfg.Listen)
	first := readStatsSnapshot(t, *cfg.StatsFile)
	if first.Chunk.PutRequestsTotal != 6 || first.Chunk.CacheInterceptedTotal != 5 || first.Chunk.S3HeadCallsTotal != 2 || first.Chunk.S3PutCallsTotal != 1 || first.ServiceState != "stopped" {
		t.Fatalf("first metrics: %+v", first)
	}
	cfg.Listen = runTestAddress(t)
	cancel, done = startRunTest(t, cfg, Options{})
	conn = waitForRunTestStore(t, cfg.Listen)
	_ = conn.Close()
	put(cfg.Listen, false)
	finishRunTest(t, cancel, done, cfg.Listen)
	second := readStatsSnapshot(t, *cfg.StatsFile)
	if second.CounterEpoch != first.CounterEpoch || second.InstanceID == first.InstanceID || second.Chunk.PutRequestsTotal != 7 || second.Chunk.CacheInterceptedTotal != 6 || second.MetadataCache.Entries != 1 {
		t.Fatalf("restart metrics: %+v", second)
	}
	if second.Chunk.DedupRatePercent == nil || *second.Chunk.DedupRatePercent != 85.71 || second.Chunk.CacheInterceptRatePercent == nil || *second.Chunk.CacheInterceptRatePercent != 85.71 {
		t.Fatalf("RPC rates: %+v", second.Chunk)
	}
	if heads.Load() != 2 || puts.Load() != 1 || second.Chunk.S3HeadCallsTotal != 2 || second.Chunk.S3PutCallsTotal != 1 {
		t.Fatalf("restart unexpectedly queried S3: heads=%d puts=%d stats=%+v", heads.Load(), puts.Load(), second)
	}
}

func TestStatsFileRequiresTmpfsForFSBackend(t *testing.T) {
	dir, err := os.MkdirTemp("/var/tmp", "fs-metrics-parent-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if err := tmpfsfile.Check(dir); !errors.Is(err, tmpfsfile.ErrNotTmpfs) {
		t.Skip("requires non-tmpfs /var/tmp")
	}
	// Runtime validation must apply even though this is not an S3 backend.
	cfg := Config{Backend: "fs", FS: &storeconfig.FSConfig{Root: dir}, StatsFile: stringPtr(filepath.Join(dir, "missing", "metrics.json")), StatsFileInterval: "1s"}
	f, err := newStatsFile(cfg, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		f.close()
		t.Fatal("FS metrics accepted non-tmpfs parent")
	}
	if !strings.Contains(err.Error(), "not on tmpfs") {
		t.Skipf("requires non-tmpfs /var/tmp: %v", err)
	}
	if _, err = os.Stat(filepath.Dir(*cfg.StatsFile)); !os.IsNotExist(err) {
		t.Fatal("invalid stats_file created directory debris")
	}
}
