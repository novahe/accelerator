package s3

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func refreshStore(t *testing.T, f s3Client, ttl time.Duration, qps float64) *Store {
	cfg := metadataConfig(metadataDir(t))
	cfg.ChunkMetadataCache.MaxTTL = ttl
	cfg.ChunkMetadataCache.RefreshAhead = &ChunkRefreshConfig{MaxQPS: qps}
	return metadataStore(t, f, cfg)
}

func entryFor(s *Store, key store.ContentKey) *cacheEntry {
	c := s.metadata
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries[c.objectID("G1", key)]
}

func dueEntry(t *testing.T, s *Store, key store.ContentKey) *cacheEntry {
	t.Helper()
	c := s.metadata
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[c.objectID("G1", key)]
	if e == nil {
		t.Fatal("missing cache entry")
	}
	sh := c.shard(e.id)
	sh.mu.Lock()
	e.refreshAt = c.now().Add(-time.Second)
	sh.mu.Unlock()
	return e
}

func awaitRefresh(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("verification did not complete")
		}
		time.Sleep(time.Millisecond)
	}
}

type refreshHeadStart struct {
	key string
	at  time.Time
}

type refreshBarrier struct {
	*fakeS3
	block   atomic.Bool
	active  atomic.Int32
	peak    atomic.Int32
	entered chan refreshHeadStart
	release chan struct{}
}

func newRefreshBarrier() *refreshBarrier {
	return &refreshBarrier{fakeS3: newFakeS3(), entered: make(chan refreshHeadStart, 32), release: make(chan struct{}, 32)}
}

func (f *refreshBarrier) Head(ctx context.Context, key string) (*ObjectMeta, error) {
	if f.block.Load() {
		n := f.active.Add(1)
		defer f.active.Add(-1)
		for old := f.peak.Load(); n > old && !f.peak.CompareAndSwap(old, n); old = f.peak.Load() {
		}
		f.entered <- refreshHeadStart{key, time.Now()}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.release:
		}
	}
	return f.fakeS3.Head(ctx, key)
}

func enteredRefresh(t *testing.T, f *refreshBarrier) refreshHeadStart {
	t.Helper()
	select {
	case event := <-f.entered:
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not start HEAD")
		return refreshHeadStart{}
	}
}

func TestAccessRefreshOneWorkerQueueAndDedup(t *testing.T) {
	f := newRefreshBarrier()
	s := refreshStore(t, f, time.Hour, 1000)
	one, two := []byte("one"), []byte("two")
	k1, k2 := keyFromBytes(one), keyFromBytes(two)
	writeS3Object(t, s, "G1", k1, one, nil, true)
	writeS3Object(t, s, "G1", k2, two, nil, true)
	e1, e2 := dueEntry(t, s, k1), dueEntry(t, s, k2)
	f.block.Store(true)
	if !cacheExists(t, s, k1, nil) {
		t.Fatal("soft-window hit failed")
	}
	first := enteredRefresh(t, f)
	var wg sync.WaitGroup
	failed := atomic.Bool{}
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, key := range []store.ContentKey{k1, k2} {
				hit, err := s.Exists(context.Background(), "G1", store.PartitionChunk, key, nil)
				if !hit || err != nil {
					failed.Store(true)
				}
			}
		}()
	}
	wg.Wait()
	if failed.Load() || s.MetadataCacheStats().RefreshQueueEntries != 1 || f.peak.Load() != 1 {
		t.Fatalf("hit blocked or duplicate tasks: %+v peak=%d", s.MetadataCacheStats(), f.peak.Load())
	}
	f.release <- struct{}{}
	second := enteredRefresh(t, f)
	if first.key == second.key || f.peak.Load() != 1 {
		t.Fatal("duplicate HEAD or more than one worker")
	}
	f.release <- struct{}{}
	awaitRefresh(t, func() bool { return !e1.refreshPending.Load() && !e2.refreshPending.Load() })
	for key, old := range map[store.ContentKey]*cacheEntry{k1: e1, k2: e2} {
		next := entryFor(s, key)
		if next == old || next == nil || next.confirmed <= old.confirmed || next.expires <= old.expires {
			t.Fatal("HEAD did not renew the persisted confirmation")
		}
		if next.lru != old.lru || !next.visited.Load() {
			t.Fatal("refresh changed eviction position/visited state")
		}
		b := readMetadataRecord(t, s.metadata, next.id)
		record, _, decodeErr := decodeCacheRecord(b)
		if decodeErr != nil || record.expires != next.expires {
			t.Fatal("refresh was not persisted")
		}
	}
	if f.hits.head.Load() != 4 || f.hits.put.Load() != 2 {
		t.Fatalf("unexpected remote calls: head=%d put=%d", f.hits.head.Load(), f.hits.put.Load())
	}
}

func TestAccessRefreshDoesNothingWithoutAccess(t *testing.T) {
	f := newFakeS3()
	s := refreshStore(t, f, time.Hour, 1000)
	data := []byte("idle")
	key := keyFromBytes(data)
	writeS3Object(t, s, "G1", key, data, nil, true)
	e := dueEntry(t, s, key)
	time.Sleep(30 * time.Millisecond)
	if f.hits.head.Load() != 1 || e.refreshPending.Load() {
		t.Fatal("verification ran without a cache access")
	}
	cacheExists(t, s, key, nil)
	awaitRefresh(t, func() bool { return !e.refreshPending.Load() && entryFor(s, key) != e })
}

func TestAccessRefreshHardExpiredDoesNotServeStale(t *testing.T) {
	f := newFakeS3()
	s := refreshStore(t, f, 40*time.Millisecond, 1000)
	data := []byte("expire")
	key := keyFromBytes(data)
	writeS3Object(t, s, "G1", key, data, nil, true)
	if err := f.Delete(context.Background(), s.objectKey(store.PartitionChunk, "G1", key)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	if cacheExists(t, s, key, nil) || f.hits.head.Load() != 2 {
		t.Fatal("hard-expired record served without synchronous HEAD")
	}
}

func TestAccessRefreshNotFoundOrChangedSize(t *testing.T) {
	for _, missing := range []bool{true, false} {
		t.Run(map[bool]string{true: "not-found", false: "size-change"}[missing], func(t *testing.T) {
			f := newFakeS3()
			s := refreshStore(t, f, time.Hour, 1000)
			data := []byte("original")
			key := keyFromBytes(data)
			writeS3Object(t, s, "G1", key, data, nil, true)
			name := s.objectKey(store.PartitionChunk, "G1", key)
			if missing {
				_ = f.Delete(context.Background(), name)
			} else {
				_, _ = f.Put(context.Background(), name, []byte("changed size"), PutOptions{})
			}
			e := dueEntry(t, s, key)
			size := int64(len(data))
			if !cacheExists(t, s, key, &size) {
				t.Fatal("soft hit unexpectedly blocked on verification")
			}
			awaitRefresh(t, func() bool { return !e.refreshPending.Load() && entryFor(s, key) == nil })
			if cacheExists(t, s, key, &size) || s.MetadataCacheStats().ErrorsTotal != 0 {
				t.Fatal("invalid positive retained or NotFound treated as service failure")
			}
			saved := s.metadata.cfg
			_ = s.Close()
			restarted := metadataStore(t, f, Config{Bucket: "bucket", Prefix: "test", ChunkMetadataCache: &saved})
			if cacheExists(t, restarted, key, &size) {
				t.Fatal("known-invalid record resurrected after restart")
			}

		})
	}
}

func TestAccessRefreshFailureKeepsDeadlineAndBacksOff(t *testing.T) {
	f := newFakeS3()
	s := refreshStore(t, f, time.Hour, 1000)
	data := []byte("failure")
	key := keyFromBytes(data)
	writeS3Object(t, s, "G1", key, data, nil, true)
	e := dueEntry(t, s, key)
	f.mu.Lock()
	f.failNextHead = errors.New("injected 503")
	f.mu.Unlock()
	cacheExists(t, s, key, nil)
	awaitRefresh(t, func() bool { return !e.refreshPending.Load() && s.MetadataCacheStats().ErrorsTotal == 1 })
	for i := 0; i < 20; i++ {
		cacheExists(t, s, key, nil)
	}
	if entryFor(s, key) != e || f.hits.head.Load() != 2 || s.MetadataCacheStats().RefreshQueueEntries != 0 {
		t.Fatal("failure renewed entry or repeated access ignored backoff")
	}
}

func TestAccessRefreshOldResultCannotPublish(t *testing.T) {
	for _, retire := range []bool{false, true} {
		t.Run(map[bool]string{false: "newer-version", true: "generation-retired"}[retire], func(t *testing.T) {
			f := newRefreshBarrier()
			s := refreshStore(t, f, time.Hour, 1000)
			s.UpdateGenerations([]store.Generation{"G1"})
			data := []byte("version")
			key := keyFromBytes(data)
			writeS3Object(t, s, "G1", key, data, nil, true)
			e := dueEntry(t, s, key)
			f.block.Store(true)
			cacheExists(t, s, key, nil)
			enteredRefresh(t, f)
			var expected *cacheEntry
			if retire {
				s.UpdateGenerations([]store.Generation{"G2"})
			} else {
				c := s.metadata
				c.remember("G1", key, int64(len(data)), c.token("G1"), time.Now())
				expected = entryFor(s, key)
			}
			f.release <- struct{}{}
			awaitRefresh(t, func() bool { return !e.refreshPending.Load() })
			if entryFor(s, key) != expected {
				t.Fatal("old refresh resurrected/overwrote an entry")
			}
		})
	}
}

func TestAccessRefreshShutdownCancelsWorkerAndQueue(t *testing.T) {
	f := newRefreshBarrier()
	s := refreshStore(t, f, time.Hour, 1000)
	for _, data := range [][]byte{[]byte("a"), []byte("b")} {
		key := keyFromBytes(data)
		writeS3Object(t, s, "G1", key, data, nil, true)
		dueEntry(t, s, key)
	}
	f.block.Store(true)
	cacheExists(t, s, keyFromBytes([]byte("a")), nil)
	enteredRefresh(t, f)
	cacheExists(t, s, keyFromBytes([]byte("b")), nil)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if f.active.Load() != 0 || s.MetadataCacheStats().RefreshQueueEntries != 0 || s.MetadataCacheStats().ErrorsTotal != 0 {
		t.Fatal("worker/queue survived close or normal cancellation counted as an error")
	}
}

func TestAccessRefreshRateLimit(t *testing.T) {
	f := newRefreshBarrier()
	s := refreshStore(t, f, time.Hour, 10)
	for _, data := range [][]byte{[]byte("a"), []byte("b")} {
		key := keyFromBytes(data)
		writeS3Object(t, s, "G1", key, data, nil, true)
		dueEntry(t, s, key)
	}
	f.block.Store(true)
	cacheExists(t, s, keyFromBytes([]byte("a")), nil)
	first := enteredRefresh(t, f)
	cacheExists(t, s, keyFromBytes([]byte("b")), nil)
	f.release <- struct{}{}
	second := enteredRefresh(t, f)
	f.release <- struct{}{}
	if second.at.Sub(first.at) < 90*time.Millisecond {
		t.Fatal("SDK calls exceeded configured rate")
	}
}

func TestAccessRefreshQueueBoundAndRetryOnNextAccess(t *testing.T) {
	f := newRefreshBarrier()
	cfg := metadataConfig(metadataDir(t))
	cfg.ChunkMetadataCache.MaxEntries = 1
	cfg.ChunkMetadataCache.RefreshAhead = &ChunkRefreshConfig{MaxQPS: 1000}
	s := metadataStore(t, f, cfg)
	a := []byte("first queued")
	k1 := keyFromBytes(a)
	writeS3Object(t, s, "G1", k1, a, nil, true)
	dueEntry(t, s, k1)
	f.block.Store(true)
	cacheExists(t, s, k1, nil)
	enteredRefresh(t, f)
	var last store.ContentKey
	for _, data := range [][]byte{[]byte("second queued"), []byte("queue full")} {
		last = keyFromBytes(data)
		_, _ = f.fakeS3.Put(context.Background(), s.objectKey(store.PartitionChunk, "G1", last), data, PutOptions{})
		c := s.metadata
		c.remember("G1", last, int64(len(data)), c.token("G1"), time.Now())
		dueEntry(t, s, last)
		if !cacheExists(t, s, last, nil) {
			t.Fatal("queue pressure failed foreground hit")
		}
	}
	if s.MetadataCacheStats().RefreshQueueEntries != 1 || entryFor(s, last).refreshPending.Load() {
		t.Fatal("unbounded queue or rejected job stuck as pending")
	}
	f.release <- struct{}{}
	awaitRefresh(t, func() bool { return s.MetadataCacheStats().RefreshQueueEntries == 0 && f.active.Load() == 0 })
	cacheExists(t, s, last, nil)
	enteredRefresh(t, f)
	f.release <- struct{}{}
	if f.peak.Load() != 1 {
		t.Fatal("more than one worker")
	}
}

func TestAccessRefreshCannotExtendPastHardDeadlineOnFailure(t *testing.T) {
	f := newRefreshBarrier()
	s := refreshStore(t, f, 80*time.Millisecond, 1000)
	data := []byte("hard deadline")
	key := keyFromBytes(data)
	writeS3Object(t, s, "G1", key, data, nil, true)
	e := dueEntry(t, s, key)
	f.block.Store(true)
	cacheExists(t, s, key, nil)
	enteredRefresh(t, f)
	awaitRefresh(t, func() bool { return !e.refreshPending.Load() })
	f.block.Store(false)
	before, _ := s.ChunkIOStats()
	if !cacheExists(t, s, key, nil) {
		t.Fatal("foreground could not verify remote object")
	}
	after, _ := s.ChunkIOStats()
	if after != before+1 || entryFor(s, key) == e {
		t.Fatal("failed background verification kept hard-expired positive alive")
	}
}

func TestAccessRefreshPersistFailureBypassesCache(t *testing.T) {
	f := newFakeS3()
	s := refreshStore(t, f, time.Hour, 1000)
	data := []byte("disk fault")
	key := keyFromBytes(data)
	writeS3Object(t, s, "G1", key, data, nil, true)
	e := dueEntry(t, s, key)
	if err := s.metadata.db.Close(); err != nil {
		t.Fatal(err)
	}
	cacheExists(t, s, key, nil)
	awaitRefresh(t, func() bool { return !e.refreshPending.Load() && s.MetadataCacheStats().State == "bypass" })
	if !cacheExists(t, s, key, nil) || s.MetadataCacheStats().ErrorsTotal != 1 || f.hits.head.Load() != 3 {
		t.Fatal("cache publication failure affected remote data or was counted twice")
	}
}

func TestAccessRefreshScheduleSurvivesRestart(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	cfg.ChunkMetadataCache.MaxEntries = 256
	cfg.ChunkMetadataCache.MaxTTL = 192 * time.Hour
	cfg.ChunkMetadataCache.RefreshAhead = &ChunkRefreshConfig{MaxQPS: 1000}
	f := newFakeS3()
	s := metadataStore(t, f, cfg)
	keys := make([]store.ContentKey, 128)
	for i := range keys {
		data := []byte{byte(i)}
		keys[i] = keyFromBytes(data)
		writeS3Object(t, s, "G1", keys[i], data, nil, true)
	}
	_ = s.Close()
	cfg.ChunkMetadataCache.MaxTTL = 168 * time.Hour
	short := metadataStore(t, f, cfg)
	times := make([]time.Time, len(keys))
	buckets := [8]int{}
	for i, key := range keys {
		e := entryFor(short, key)
		if e.expires != time.Unix(0, e.confirmed).Add(168*time.Hour).UnixNano() {
			t.Fatal("old record was not capped by new max_ttl")
		}
		age := e.refreshAt.Sub(time.Unix(0, e.confirmed))
		if age < 126*time.Hour || age > 147*time.Hour {
			t.Fatalf("refresh outside soft window: %s", age)
		}
		bucket := min(7, int((age-126*time.Hour)*8/(21*time.Hour)))
		buckets[bucket]++
		times[i] = e.refreshAt
	}
	for _, n := range buckets {
		if n == 0 {
			t.Fatal("cohort did not spread across refresh window")
		}
	}
	seed := short.metadata.refresh.seed
	_ = short.Close()
	cfg.ChunkMetadataCache.MaxTTL = 240 * time.Hour
	next := metadataStore(t, f, cfg)
	if !bytes.Equal(seed[:], next.metadata.refresh.seed[:]) {
		t.Fatal("restart replaced refresh seed")
	}
	for i, key := range keys {
		if entryFor(next, key).refreshAt.UnixNano() != times[i].UnixNano() {
			t.Fatal("restart changed soft schedule or extended the capped record")
		}
	}
	if f.hits.head.Load() != int64(len(keys)) {
		t.Fatal("reload/idle entries issued proactive HEAD")
	}
}
