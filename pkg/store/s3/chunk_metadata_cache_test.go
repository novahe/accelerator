package s3

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/util/tmpfsfile"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func metadataDir(t *testing.T) string {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("tmpfs requires Linux")
	}
	dir, err := os.MkdirTemp("/dev/shm", "store-metadata-test-")
	if err != nil {
		t.Skipf("tmpfs unavailable: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err = tmpfsfile.Dir(dir); err != nil {
		t.Fatal(err)
	}
	return dir
}
func metadataConfig(dir string) Config {
	return Config{Bucket: "bucket", Prefix: "test", ChunkMetadataCache: &ChunkMetadataCacheConfig{Dir: dir, Namespace: CacheNamespace("https://test", "region", "bucket", "test", true), MaxTTL: 10 * time.Minute, MaxEntries: 10}}
}
func metadataStore(t *testing.T, f s3Client, cfg Config) *Store {
	t.Helper()
	s, err := New(context.Background(), f, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}
func cacheExists(t *testing.T, s *Store, k store.ContentKey, size *int64) bool {
	t.Helper()
	v, err := s.Exists(context.Background(), "G1", store.PartitionChunk, k, size)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestMetadataCacheDedupReloadAndOriginalDeadline(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	f := newFakeS3()
	s := metadataStore(t, f, cfg)
	data := []byte("persist chunk presence")
	key := keyFromBytes(data)
	size := int64(len(data))
	if !writeS3Object(t, s, "G1", key, data, &size, true) {
		t.Fatal("not new")
	}
	for i := 0; i < 20; i++ {
		if !cacheExists(t, s, key, &size) {
			t.Fatal("miss")
		}
	}
	if f.hits.head.Load() != 1 || f.hits.put.Load() != 1 {
		t.Fatalf("unexpected calls/stats: %+v", s.MetadataCacheStats())
	}
	e := s.metadata.entries[s.metadata.objectID("G1", key)]
	original := e.expires
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.ChunkMetadataCache.MaxTTL = time.Hour // Increasing the configured TTL must not renew existing records.
	next := metadataStore(t, f, cfg)
	if next.MetadataCacheStats().ReloadedEntries != 1 || !cacheExists(t, next, key, &size) || f.hits.head.Load() != 1 {
		t.Fatalf("reload did not avoid HEAD: %+v", next.MetadataCacheStats())
	}
	e = next.metadata.entries[next.metadata.objectID("G1", key)]
	if e.expires != original {
		t.Fatalf("restart renewed MaxTTL: %d != %d", e.expires, original)
	}
	if _, err := New(context.Background(), f, cfg); err == nil {
		t.Fatal("second cache writer acquired lock")
	}
}

func TestMetadataCacheTTLDoesNotSlide(t *testing.T) {
	s := metadataStore(t, newFakeS3(), metadataConfig(metadataDir(t)))
	data := []byte("clock")
	key := keyFromBytes(data)
	writeS3Object(t, s, "G1", key, data, nil, true)
	c := s.metadata
	c.mu.Lock()
	deadline := c.entries[c.objectID("G1", key)].deadline
	c.now = func() time.Time { return deadline.Add(-time.Nanosecond) }
	c.mu.Unlock()
	for i := 0; i < 5; i++ {
		if !cacheExists(t, s, key, nil) {
			t.Fatal("early expiry")
		}
	}
	c.mu.Lock()
	c.now = func() time.Time { return deadline }
	c.mu.Unlock()
	if !cacheExists(t, s, key, nil) {
		t.Fatal("remote object missing")
	}
	heads, _ := s.ChunkIOStats()
	if heads != 2 {
		t.Fatalf("expired cache avoided HEAD: %d", heads)
	}
}

func TestMetadataCacheSizeAndNonChunkIsolation(t *testing.T) {
	f := newFakeS3()
	s := metadataStore(t, f, metadataConfig(metadataDir(t)))
	data := []byte("sized")
	key := keyFromBytes(data)
	writeS3Object(t, s, "G1", key, data, nil, true)
	wrong := int64(0)
	if cacheExists(t, s, key, &wrong) {
		t.Fatal("size mismatch accepted")
	}
	if !cacheExists(t, s, key, nil) {
		t.Fatal("nil-size miss")
	}
	other := store.Generation("G2")
	if v, err := s.Exists(context.Background(), other, store.PartitionChunk, key, nil); v || err != nil {
		t.Fatalf("cross-generation hit: %v %v", v, err)
	}
	before := f.hits.head.Load()
	for _, p := range []store.Partition{store.PartitionManifest, store.PartitionBlob} {
		for i := 0; i < 2; i++ {
			if _, err := s.Exists(context.Background(), "G1", p, key, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	if f.hits.head.Load() != before+4 {
		t.Fatal("non-chunk used metadata cache")
	}
	neg := int64(-1)
	if _, err := s.Exists(context.Background(), "G1", store.PartitionChunk, key, &neg); err == nil {
		t.Fatal("negative size accepted")
	}
	empty := []byte{}
	ek := keyFromBytes(empty)
	zero := int64(0)
	writeS3Object(t, s, "G1", ek, empty, &zero, true)
	if !cacheExists(t, s, ek, &zero) {
		t.Fatal("empty chunk not cached")
	}
}

func TestMetadataCacheNoNegativeOrFailedWritePublication(t *testing.T) {
	f := newFakeS3()
	s := metadataStore(t, f, metadataConfig(metadataDir(t)))
	key := keyFromBytes([]byte("absent"))
	for i := 0; i < 2; i++ {
		if cacheExists(t, s, key, nil) {
			t.Fatal("negative hit")
		}
	}
	if f.hits.head.Load() != 2 {
		t.Fatal("negative cache introduced")
	}
	failure := errors.New("head denied")
	f.failNextHead = failure
	if _, err := s.Exists(context.Background(), "G1", store.PartitionChunk, key, nil); !errors.Is(err, failure) {
		t.Fatalf("HEAD error lost: %v", err)
	}
	h, _ := s.OpenPut("G1", store.PartitionChunk, key, nil)
	_, _ = h.Write([]byte("absent"))
	f.failNextPut = failure
	if _, err := h.Commit(key, key); !errors.Is(err, failure) {
		t.Fatalf("PUT failure lost: %v", err)
	}
	if s.MetadataCacheStats().Entries != 0 {
		t.Fatal("failed upload cached")
	}
	h, _ = s.OpenPut("G1", store.PartitionChunk, key, nil)
	_, _ = h.Write([]byte("absent"))
	_ = h.Abort()
	if s.MetadataCacheStats().Entries != 0 {
		t.Fatal("abort cached")
	}
}

func TestMetadataCacheConcurrentCommitUploadsOnce(t *testing.T) {
	f := newFakeS3()
	s := metadataStore(t, f, metadataConfig(metadataDir(t)))
	data := []byte("concurrent")
	key := keyFromBytes(data)
	const n = 32
	handles := make([]store.PutHandle, n)
	for i := range handles {
		handles[i], _ = s.OpenPut("G1", store.PartitionChunk, key, nil)
		_, _ = handles[i].Write(data)
	}
	start := make(chan struct{})
	results := make(chan error, n)
	var fresh atomic.Int64
	for _, h := range handles {
		go func() {
			<-start
			v, err := h.Commit(key, key)
			if v {
				fresh.Add(1)
			}
			results <- err
		}()
	}
	close(start)
	for i := 0; i < n; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if fresh.Load() != 1 || f.hits.put.Load() != 1 || f.hits.head.Load() != 1 {
		t.Fatalf("duplicate concurrent PUT: fresh=%d heads=%d puts=%d cache=%+v", fresh.Load(), f.hits.head.Load(), f.hits.put.Load(), s.MetadataCacheStats())
	}
	s.gates.mu.Lock()
	defer s.gates.mu.Unlock()
	if len(s.gates.gates) != 0 {
		t.Fatal("gate leak")
	}
}

func TestMetadataCacheGateCancellation(t *testing.T) {
	var gates keyGates
	var id [32]byte
	unlock, err := gates.acquire(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		release, err := gates.acquire(ctx, id)
		if release != nil {
			release()
		}
		done <- err
	}()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("wait not cancelled: %v", err)
	}
	unlock()
	gates.mu.Lock()
	defer gates.mu.Unlock()
	if len(gates.gates) != 0 {
		t.Fatal("cancelled gate retained")
	}
}

type blockedMetadataS3 struct {
	*fakeS3
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *blockedMetadataS3) Head(ctx context.Context, key string) (*ObjectMeta, error) {
	f.once.Do(func() {
		close(f.entered)
		select {
		case <-f.release:
		case <-ctx.Done():
		}
	})
	return f.fakeS3.Head(ctx, key)
}
func TestMetadataCacheGenerationEpochRejectsInflightConfirmation(t *testing.T) {
	f := &blockedMetadataS3{fakeS3: newFakeS3(), entered: make(chan struct{}), release: make(chan struct{})}
	s := metadataStore(t, f, metadataConfig(metadataDir(t)))
	s.UpdateGenerations([]store.Generation{"G1"})
	data := []byte("epoch")
	key := keyFromBytes(data)
	_, _ = f.fakeS3.Put(context.Background(), s.objectKey(store.PartitionChunk, "G1", key), data, PutOptions{})
	done := make(chan error, 1)
	go func() { _, err := s.Exists(context.Background(), "G1", store.PartitionChunk, key, nil); done <- err }()
	<-f.entered
	s.UpdateGenerations([]store.Generation{"G2"})
	s.UpdateGenerations([]store.Generation{"G1"})
	close(f.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if s.MetadataCacheStats().Entries != 0 {
		t.Fatal("old epoch filled a new generation")
	}
}

func TestMetadataCacheMemoryHitAndCorruptReload(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	f := newFakeS3()
	s := metadataStore(t, f, cfg)
	data := []byte("corrupt")
	key := keyFromBytes(data)
	writeS3Object(t, s, "G1", key, data, nil, true)
	overwriteMetadataRecord(t, s.metadata, s.metadata.objectID("G1", key), []byte("truncated"))
	size := int64(len(data))
	if !cacheExists(t, s, key, &size) || f.hits.head.Load() != 1 {
		t.Fatal("memory hit read record")
	}
	_ = s.Close()
	next := metadataStore(t, f, cfg)
	if next.MetadataCacheStats().ReloadedEntries != 0 {
		t.Fatal("corrupt record restored")
	}
	if !cacheExists(t, next, key, &size) || f.hits.head.Load() != 2 {
		t.Fatal("reload did not repair via HEAD")
	}
}

func TestMetadataCacheHotHitSkipsGate(t *testing.T) {
	f := newFakeS3()
	s := metadataStore(t, f, metadataConfig(metadataDir(t)))
	data := []byte("hot")
	key := keyFromBytes(data)
	writeS3Object(t, s, "G1", key, data, nil, true)
	unlock, err := s.gates.acquire(context.Background(), s.metadata.objectID("G1", key))
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ok, err := s.Exists(ctx, "G1", store.PartitionChunk, key, nil)
	if err != nil || !ok {
		t.Fatalf("hit waited on gate: %v", err)
	}
	h, err := s.OpenPut("G1", store.PartitionChunk, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.Write(data); err != nil {
		t.Fatal(err)
	}
	created, err := h.Commit(key, key)
	if err != nil || created {
		t.Fatalf("hot Commit: created=%v err=%v", created, err)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := s.Exists(canceled, "G1", store.PartitionChunk, key, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled hit: %v", err)
	}
}

func TestMetadataCacheCapacityAndReloadLimit(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	cfg.ChunkMetadataCache.MaxEntries = 2
	f := newFakeS3()
	s := metadataStore(t, f, cfg)
	var keys []store.ContentKey
	for _, data := range [][]byte{[]byte("one"), []byte("two"), []byte("three")} {
		key := keyFromBytes(data)
		keys = append(keys, key)
		writeS3Object(t, s, "G1", key, data, nil, true)
	}
	if stats := s.MetadataCacheStats(); stats.Entries != 2 {
		t.Fatalf("capacity: %+v", stats)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.ChunkMetadataCache.MaxEntries = 1
	next := metadataStore(t, f, cfg)
	if next.MetadataCacheStats().Entries != 1 {
		t.Fatal("restart ignored lowered limit")
	}
	if count := persistedRecordCount(t, next.metadata); count != 1 {
		t.Fatalf("database exceeds reduced limit: %d", count)
	}
}

func TestMetadataCacheReloadRejectsExpiredCorruptAndRetired(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	f := newFakeS3()
	s := metadataStore(t, f, cfg)
	data := []byte("expired")
	key := keyFromBytes(data)
	writeS3Object(t, s, "G1", key, data, nil, true)
	c := s.metadata
	id := c.objectID("G1", key)
	e := &cacheEntry{generation: "G1", key: key, confirmed: time.Now().Add(-2 * time.Hour).UnixNano(), expires: time.Now().Add(-time.Hour).UnixNano()}
	overwriteMetadataRecord(t, c, id, encodeCacheRecord(e, int64(len(data))))
	_ = s.Close()
	next := metadataStore(t, f, cfg)
	if next.MetadataCacheStats().Entries != 0 {
		t.Fatal("expired record restored")
	}
	_ = next.Close()
	s = metadataStore(t, f, cfg)
	writeS3Object(t, s, "G1", key, data, nil, true)
	_ = s.Close()
	next = metadataStore(t, f, cfg)
	next.UpdateGenerations([]store.Generation{"G2"})
	if next.MetadataCacheStats().Entries-next.MetadataCacheStats().PendingDeleteEntries != 0 {
		t.Fatal("retired generation retained on restart")
	}
}

func TestMetadataCacheRequiresTmpfs(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux")
	}
	dir, err := os.MkdirTemp("/var/tmp", "store-cache-non-tmpfs-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if err = tmpfsfile.Dir(dir); err == nil {
		t.Skip("/var/tmp itself is tmpfs")
	}
	if s, err := New(context.Background(), newFakeS3(), metadataConfig(dir)); err == nil {
		_ = s.Close()
		t.Fatal("accepted non-tmpfs cache")
	}
}

func TestMetadataCacheShorterTTLIsPersistedAndLRUTouches(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	cfg.ChunkMetadataCache.MaxEntries = 2
	f := newFakeS3()
	s := metadataStore(t, f, cfg)
	one, two, three := []byte("first"), []byte("second"), []byte("third")
	k1, k2, k3 := keyFromBytes(one), keyFromBytes(two), keyFromBytes(three)
	writeS3Object(t, s, "G1", k1, one, nil, true)
	writeS3Object(t, s, "G1", k2, two, nil, true)
	if !cacheExists(t, s, k1, nil) {
		t.Fatal("touch missed")
	}
	writeS3Object(t, s, "G1", k3, three, nil, true)
	c := s.metadata
	c.mu.Lock()
	_, kept := c.entries[c.objectID("G1", k1)]
	_, evicted := c.entries[c.objectID("G1", k2)]
	c.mu.Unlock()
	if !kept || evicted {
		t.Fatal("LRU ignored cache access")
	}
	_ = s.Close()
	cfg.ChunkMetadataCache.MaxTTL = time.Minute
	short := metadataStore(t, f, cfg)
	b := readMetadataRecord(t, short.metadata, short.metadata.objectID("G1", k1))
	e, _, err := decodeCacheRecord(b)
	if err != nil {
		t.Fatal(err)
	}
	if e.expires != time.Unix(0, e.confirmed).Add(time.Minute).UnixNano() {
		t.Fatal("shorter TTL was not persisted")
	}
	_ = short.Close()
	cfg.ChunkMetadataCache.MaxTTL = time.Hour
	long := metadataStore(t, f, cfg)
	long.metadata.mu.Lock()
	expires := long.metadata.entries[long.metadata.objectID("G1", k1)].expires
	long.metadata.mu.Unlock()
	if expires != e.expires {
		t.Fatal("later restart extended shortened TTL")
	}
}

func TestMetadataCacheOfflinePurgeClearsPersistentPresence(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	f := newFakeS3()
	s := metadataStore(t, f, cfg)
	data := []byte("purge")
	k := keyFromBytes(data)
	writeS3Object(t, s, "G1", k, data, nil, true)
	if _, err := LockAndClearMetadataCache(cfg.ChunkMetadataCache.Dir, cfg.ChunkMetadataCache.Namespace); err == nil {
		t.Fatal("offline purge raced live cache")
	}
	_ = s.Close()
	release, err := LockAndClearMetadataCache(cfg.ChunkMetadataCache.Dir, cfg.ChunkMetadataCache.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if next, err := New(context.Background(), f, cfg); err == nil {
		_ = next.Close()
		t.Fatal("new process opened cache during wipe")
	}
	if err = f.Delete(context.Background(), s.objectKey(store.PartitionChunk, "G1", k)); err != nil {
		t.Fatal(err)
	}
	_ = release()
	next := metadataStore(t, f, cfg)
	if cacheExists(t, next, k, nil) {
		t.Fatal("wiped data reloaded as present")
	}
}

func TestMetadataCachePublicationFailureDoesNotFailSuccessfulPUT(t *testing.T) {
	f := newFakeS3()
	s := metadataStore(t, f, metadataConfig(metadataDir(t)))
	if err := s.metadata.db.Close(); err != nil {
		t.Fatal(err)
	}
	data := []byte("remote upload survives cache write failure")
	key := keyFromBytes(data)
	if !writeS3Object(t, s, "G1", key, data, nil, true) {
		t.Fatal("successful S3 PUT lost")
	}
	found, body, err := s.Get(context.Background(), "G1", store.PartitionChunk, key)
	if err != nil || !found || string(body) != string(data) || s.MetadataCacheStats().State != "bypass" {
		t.Fatalf("data lost on cache failure: %v %v %+v", found, err, s.MetadataCacheStats())
	}
}

func TestMetadataCacheExplicitAdminInvalidatesPersistentRecords(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	f := newFakeS3()
	s := metadataStore(t, f, cfg)
	data := []byte("admin")
	key := keyFromBytes(data)
	writeS3Object(t, s, "G1", key, data, nil, true)
	writeS3Object(t, s, "G2", key, data, nil, true)
	if err := s.DropGeneration(context.Background(), "G1"); err != nil {
		t.Fatal(err)
	}
	if cacheExists(t, s, key, nil) {
		t.Fatal("dropped generation cached")
	}
	if err := s.Wipe(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	next := metadataStore(t, f, cfg)
	if v, err := next.Exists(context.Background(), "G2", store.PartitionChunk, key, nil); err != nil || v {
		t.Fatalf("wiped generation restored: %v %v", v, err)
	}
}

// A full queue must consume one shared stage budget, not start a fresh timeout
// for every HEAD after the previous same-key operation fails.
type stalledMetadataHead struct{ *fakeS3 }

func (f *stalledMetadataHead) Head(ctx context.Context, _ string) (*ObjectMeta, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func TestMetadataCacheStalledHeadDoesNotCreateSerialTimeoutChain(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	cfg.OpTimeout = 100 * time.Millisecond
	s := metadataStore(t, &stalledMetadataHead{newFakeS3()}, cfg)
	const n = 32
	start := make(chan struct{})
	done := make(chan error, n)
	key := keyFromBytes([]byte("stalled"))
	for i := 0; i < n; i++ {
		go func() {
			<-start
			_, err := s.Exists(context.Background(), "G1", store.PartitionChunk, key, nil)
			done <- err
		}()
	}
	close(start)
	// The old implementation takes ~3.2s. Leave ample scheduling headroom for
	// the shared 100ms budgets without relying on millisecond timing equality.
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for i := 0; i < n; i++ {
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unexpected error: %v", err)
			}
		case <-timeout.C:
			t.Fatal("same-key operations formed a timeout chain")
		}
	}
	s.gates.mu.Lock()
	defer s.gates.mu.Unlock()
	if len(s.gates.gates) != 0 {
		t.Fatal("timed-out gates retained")
	}
}

func TestMetadataCacheBudgetIncludesGateAndS3SemaphoreWait(t *testing.T) {
	for _, operation := range []string{"exists-gate", "commit-gate", "exists-semaphore"} {
		t.Run(operation, func(t *testing.T) {
			cfg := metadataConfig(metadataDir(t))
			cfg.OpTimeout = 50 * time.Millisecond
			f := newFakeS3()
			s := metadataStore(t, f, cfg)
			data := []byte("queued")
			key := keyFromBytes(data)
			if operation == "exists-semaphore" {
				for i := 0; i < cap(s.sem); i++ {
					s.sem <- struct{}{}
				}
				defer func() {
					for i := 0; i < cap(s.sem); i++ {
						<-s.sem
					}
				}()
			} else {
				release, err := s.gates.acquire(context.Background(), s.metadata.objectID("G1", key))
				if err != nil {
					t.Fatal(err)
				}
				defer release()
			}
			done := make(chan error, 1)
			go func() {
				if operation == "commit-gate" {
					h, err := s.OpenPut("G1", store.PartitionChunk, key, nil)
					if err != nil {
						done <- err
						return
					}
					_, _ = h.Write(data)
					_, err = h.Commit(key, key)
					done <- err
				} else {
					_, err := s.Exists(context.Background(), "G1", store.PartitionChunk, key, nil)
					done <- err
				}
			}()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("wait not bounded: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("queue did not observe stage timeout")
			}
			if f.hits.head.Load() != 0 || f.hits.put.Load() != 0 {
				t.Fatal("queued cancellation counted as a remote request")
			}
		})
	}
}

type metadataDeadlineS3 struct {
	*fakeS3
	headDeadline, putDeadline time.Time
}

func (f *metadataDeadlineS3) Head(ctx context.Context, k string) (*ObjectMeta, error) {
	f.headDeadline, _ = ctx.Deadline()
	return f.fakeS3.Head(ctx, k)
}
func (f *metadataDeadlineS3) Put(ctx context.Context, k string, b []byte, o PutOptions) (string, error) {
	f.putDeadline, _ = ctx.Deadline()
	return f.fakeS3.Put(ctx, k, b, o)
}
func TestMetadataCacheCommitSharesDeadlineAcrossHEADAndPUT(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	cfg.OpTimeout = time.Second
	f := &metadataDeadlineS3{fakeS3: newFakeS3()}
	s := metadataStore(t, f, cfg)
	data := []byte("shared deadline")
	key := keyFromBytes(data)
	writeS3Object(t, s, "G1", key, data, nil, true)
	if f.headDeadline.IsZero() || !f.headDeadline.Equal(f.putDeadline) {
		t.Fatalf("budget reset between HEAD and PUT: %v %v", f.headDeadline, f.putDeadline)
	}
}

func TestMetadataCacheOfflineClearSkipsNonTmpfsResidue(t *testing.T) {
	dir, err := os.MkdirTemp("/var/tmp", "cache-residue-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if err = tmpfsfile.Check(dir); !errors.Is(err, tmpfsfile.ErrNotTmpfs) {
		t.Skip("requires a non-tmpfs directory")
	}
	namespace := metadataConfig(dir).ChunkMetadataCache.Namespace
	record := filepath.Join(dir, namespace, "entries", "residue.meta")
	if err = os.MkdirAll(filepath.Dir(record), 0o700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(record, []byte("residue"), 0o600); err != nil {
		t.Fatal(err)
	}
	release, err := LockAndClearMetadataCache(dir, namespace)
	if err != nil {
		t.Fatal(err)
	}
	if err = release(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(record); err != nil {
		t.Fatal("non-tmpfs residue was mutated")
	}
	if _, err = os.Stat(filepath.Join(dir, namespace, "LOCK")); !os.IsNotExist(err) {
		t.Fatal("offline inspection created a lock")
	}
	missing := filepath.Join(dir, "not-created", "nested")
	if err = tmpfsfile.Dir(missing); !errors.Is(err, tmpfsfile.ErrNotTmpfs) {
		t.Fatalf("wrong error: %v", err)
	}
	if _, err = os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("rejected non-tmpfs start left directory debris")
	}
}

func TestMetadataCacheCountsDroppedFillsDuringCleanupBacklog(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	cfg.ChunkMetadataCache.MaxEntries = 300
	s := metadataStore(t, newFakeS3(), cfg)
	for i := 0; i < 257; i++ {
		data := []byte{byte(i), byte(i >> 8)}
		writeS3Object(t, s, "G1", keyFromBytes(data), data, nil, true)
	}
	s.UpdateGenerations([]store.Generation{"G2"})
	data := []byte("backlog does not fail remote commit")
	if !writeS3Object(t, s, "G2", keyFromBytes(data), data, nil, true) {
		t.Fatal("backlog failed successful PUT")
	}
	stats := s.MetadataCacheStats()
	if stats.FillsDroppedTotal != 1 || stats.PendingDeleteEntries != 129 || stats.State != "active" || stats.ErrorsTotal != 0 {
		t.Fatalf("dropped fill not distinguishable from failure: %+v", stats)
	}
}

func TestMetadataCacheHitDoesNotWaitForMutationCoordinator(t *testing.T) {
	f := newFakeS3()
	s := metadataStore(t, f, metadataConfig(metadataDir(t)))
	data := []byte("independent read")
	key := keyFromBytes(data)
	writeS3Object(t, s, "G1", key, data, nil, true)
	s.metadata.mu.Lock()
	done := make(chan bool, 1)
	go func() {
		hit, err := s.Exists(context.Background(), "G1", store.PartitionChunk, key, nil)
		done <- hit && err == nil
	}()
	select {
	case ok := <-done:
		s.metadata.mu.Unlock()
		if !ok {
			t.Fatal("hot lookup failed")
		}
	case <-time.After(time.Second):
		s.metadata.mu.Unlock()
		<-done
		t.Fatal("hit blocked on mutation/file I/O coordinator")
	}
}

func TestMetadataCacheSievePreservesVisitedEntry(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	cfg.ChunkMetadataCache.MaxEntries = 2
	f := newFakeS3()
	s := metadataStore(t, f, cfg)
	a, b, c := []byte("a"), []byte("b"), []byte("c")
	for _, data := range [][]byte{a, b} {
		writeS3Object(t, s, "G1", keyFromBytes(data), data, nil, true)
	}
	if !cacheExists(t, s, keyFromBytes(a), nil) {
		t.Fatal("missing a")
	}
	writeS3Object(t, s, "G1", keyFromBytes(c), c, nil, true)
	before := f.hits.head.Load()
	if !cacheExists(t, s, keyFromBytes(a), nil) || f.hits.head.Load() != before {
		t.Fatal("visited entry evicted")
	}
	if !cacheExists(t, s, keyFromBytes(b), nil) || f.hits.head.Load() != before+1 {
		t.Fatal("unvisited entry retained")
	}
}

func TestMetadataCacheSieveScanBound(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	cfg.ChunkMetadataCache.MaxEntries = 130
	f := newFakeS3()
	s := metadataStore(t, f, cfg)
	for i := 0; i < 130; i++ {
		data := []byte{byte(i)}
		key := keyFromBytes(data)
		writeS3Object(t, s, "G1", key, data, nil, true)
		if !cacheExists(t, s, key, nil) {
			t.Fatal("missing entry")
		}
	}
	before := s.MetadataCacheStats()
	data := []byte("new entry beyond scan budget")
	writeS3Object(t, s, "G1", keyFromBytes(data), data, nil, true)
	after := s.MetadataCacheStats()
	if after.Entries != 130 || after.FillsDroppedTotal != before.FillsDroppedTotal+1 {
		t.Fatalf("scan limit: %+v", after)
	}
}

// Hold remote deletion after local invalidation, while HEAD can still see data.
type metadataDropBarrier struct {
	*fakeS3
	listed  chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *metadataDropBarrier) List(ctx context.Context, prefix string, visit func(string) bool) error {
	f.once.Do(func() { close(f.listed); <-f.release })
	return f.fakeS3.List(ctx, prefix, visit)
}
func TestMetadataCacheDropCannotRefillDuringDelete(t *testing.T) {
	for _, tracked := range []bool{false, true} {
		t.Run(map[bool]string{false: "untracked", true: "tracked"}[tracked], func(t *testing.T) {
			f := &metadataDropBarrier{fakeS3: newFakeS3(), listed: make(chan struct{}), release: make(chan struct{})}
			s := metadataStore(t, f, metadataConfig(metadataDir(t)))
			if tracked {
				s.UpdateGenerations([]store.Generation{"G1", "G2"})
			}
			data := []byte("delete race")
			key := keyFromBytes(data)
			writeS3Object(t, s, "G1", key, data, nil, true)
			writeS3Object(t, s, "G2", key, data, nil, true)
			done := make(chan error, 1)
			go func() { done <- s.DropGeneration(context.Background(), "G1") }()
			<-f.listed
			exists, err := s.Exists(context.Background(), "G1", store.PartitionChunk, key, nil)
			// Release deletion before assertions so failures cannot leak a goroutine.
			close(f.release)
			dropErr := <-done
			if err != nil || !exists || dropErr != nil {
				t.Fatalf("during drop: exists=%v err=%v drop=%v", exists, err, dropErr)
			}
			exists, err = s.Exists(context.Background(), "G1", store.PartitionChunk, key, nil)
			if err != nil || exists {
				t.Fatalf("deleted object cached as present: exists=%v err=%v", exists, err)
			}
			before := f.hits.head.Load()
			exists, err = s.Exists(context.Background(), "G2", store.PartitionChunk, key, nil)
			if err != nil || !exists || f.hits.head.Load() != before {
				t.Fatalf("unrelated generation lost cache: %v", err)
			}
			s.UpdateGenerations([]store.Generation{"G1", "G2"})
			writeS3Object(t, s, "G1", key, data, nil, true)
			before = f.hits.head.Load()
			exists, err = s.Exists(context.Background(), "G1", store.PartitionChunk, key, nil)
			if err != nil || !exists || f.hits.head.Load() != before+1 {
				t.Fatal("generation refresh re-enabled dropped cache")
			}

		})
	}
}
