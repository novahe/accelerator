package s3

import (
	"container/heap"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/kuasar-sandbox/accelerator/internal/util/chunkmeta"
	"github.com/kuasar-sandbox/accelerator/internal/util/tmpfsfile"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// ChunkMetadataCacheConfig is the explicit low-level cache opt-in. The Store
// application supplies its default-on configuration; admin/raw Stores stay off.
type ChunkMetadataCacheConfig struct {
	Dir, Namespace string
	MaxTTL         time.Duration
	MaxEntries     int
	Logger         *slog.Logger
	RefreshAhead   *ChunkRefreshConfig
}

// CacheNamespace excludes credentials and binds records to an object backend.
func CacheNamespace(endpoint, region, bucket, prefix string, pathStyle bool) string {
	return chunkmeta.Namespace(endpoint, region, bucket, prefix, pathStyle)
}

type CacheCounters struct {
	ErrorsTotal       uint64 `json:"errors_total"`
	FillsDroppedTotal uint64 `json:"fills_dropped_total"`
}

func (s CacheCounters) Add(b CacheCounters) CacheCounters {
	s.ErrorsTotal += b.ErrorsTotal
	s.FillsDroppedTotal += b.FillsDroppedTotal
	return s
}

type MetadataCacheStats struct {
	CacheCounters
	Enabled              bool    `json:"-"`
	State                string  `json:"state"`
	MaxTTLSeconds        float64 `json:"-"`
	MaxEntries           int     `json:"-"`
	Entries              int     `json:"entries"`
	PendingDeleteEntries int     `json:"-"`
	ReloadedEntries      int     `json:"-"`
	RefreshQueueEntries  int     `json:"refresh_queue_entries"`
}

type cacheShard struct {
	mu      sync.RWMutex
	entries map[[32]byte]*cacheEntry
}

type cacheEntry struct {
	visited            atomic.Bool
	refreshPending     atomic.Bool
	retryAfter         atomic.Int64
	refreshAt          time.Time
	id                 [32]byte
	key                store.ContentKey
	generation         store.Generation
	confirmed, expires int64
	size               int64
	deadline           time.Time
	pos                int
	lru                *list.Element
}

type expiryHeap []*cacheEntry

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }
func (h expiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].pos = i; h[j].pos = j }
func (h *expiryHeap) Push(v any)        { e := v.(*cacheEntry); e.pos = len(*h); *h = append(*h, e) }
func (h *expiryHeap) Pop() any {
	a := *h
	e := a[len(a)-1]
	a[len(a)-1] = nil
	*h = a[:len(a)-1]
	e.pos = -1
	return e
}

type metadataCache struct {
	mu             sync.Mutex
	shards         [64]cacheShard
	disabled       atomic.Bool
	hand           *list.Element
	cfg            ChunkMetadataCacheConfig
	dir            string
	lock           *os.File
	db             *bolt.DB
	entries        map[[32]byte]*cacheEntry
	expiry         expiryHeap
	lru            list.List
	pending        []*cacheEntry
	pendingHead    int
	counters       CacheCounters
	refresh        *metadataRefresh
	reloaded       int
	tracked        bool
	generations    map[store.Generation]uint64
	dropped        map[store.Generation]bool
	nextEpoch      uint64
	bypass, closed bool
	now            func() time.Time
	stop           chan struct{}
	done           chan struct{}
	closeOnce      sync.Once
	closeErr       error
}

func newMetadataCache(cfg ChunkMetadataCacheConfig) (*metadataCache, error) {
	if cfg.RefreshAhead != nil {
		if _, err := refreshInterval(cfg.RefreshAhead.MaxQPS); err != nil {
			return nil, err
		}
	}
	if cfg.MaxTTL <= 0 || cfg.MaxEntries <= 0 || len(cfg.Namespace) != 64 {
		return nil, errors.New("s3: invalid chunk metadata cache configuration")
	}
	if _, err := hex.DecodeString(cfg.Namespace); err != nil {
		return nil, errors.New("s3: invalid cache namespace")
	}
	dir := filepath.Join(cfg.Dir, cfg.Namespace)
	if err := tmpfsfile.Dir(dir); err != nil {
		return nil, fmt.Errorf("s3 chunk metadata cache: %w", err)
	}
	lock, err := tmpfsfile.Lock(filepath.Join(dir, "LOCK"))
	if err != nil {
		return nil, err
	}
	c := &metadataCache{cfg: cfg, dir: dir, lock: lock, entries: make(map[[32]byte]*cacheEntry), nextEpoch: 1, now: time.Now, stop: make(chan struct{}), done: make(chan struct{})}
	if c.cfg.Logger == nil {
		c.cfg.Logger = slog.Default()
	}
	if cfg.RefreshAhead != nil {
		c.refresh, err = newMetadataRefresh(c, cfg.RefreshAhead.MaxQPS)
		if err != nil {
			_ = lock.Close()
			return nil, err
		}
	}
	if err = c.openDB(); err == nil {
		err = c.reload()
		if err == nil {
			err = c.compactAtStartup()
		}
		c.reloaded = len(c.entries)
	}
	if err != nil {
		if c.refresh != nil {
			c.refresh.cancel()
		}
		if c.db != nil {
			_ = c.db.Close()
		}
		_ = lock.Close()
		return nil, fmt.Errorf("s3 reload chunk metadata cache: %w", err)
	}
	go c.clean()
	return c, nil
}

func (c *metadataCache) objectID(g store.Generation, k store.ContentKey) [32]byte {
	return sha256.Sum256(append([]byte(c.cfg.Namespace+"\x00"+string(g)+"\x00"), k[:]...))
}

// The record includes generation and key so reload can validate the full object
// identity and generation membership without HEAD. It never contains body bytes.
const cacheRecordSize = 196

var recordCRC = crc32.MakeTable(crc32.Castagnoli)

func encodeCacheRecord(e *cacheEntry, size int64) []byte {
	b := make([]byte, cacheRecordSize)
	copy(b, "SCMC")
	binary.LittleEndian.PutUint16(b[4:6], 1)
	binary.LittleEndian.PutUint16(b[6:8], uint16(len(e.generation)))
	copy(b[8:136], string(e.generation))
	copy(b[136:168], e.key[:])
	binary.LittleEndian.PutUint64(b[168:176], uint64(size))
	binary.LittleEndian.PutUint64(b[176:184], uint64(e.confirmed))
	binary.LittleEndian.PutUint64(b[184:192], uint64(e.expires))
	binary.LittleEndian.PutUint32(b[192:], crc32.Checksum(b[:192], recordCRC))
	return b
}
func decodeCacheRecord(b []byte) (*cacheEntry, int64, error) {
	if len(b) != cacheRecordSize || string(b[:4]) != "SCMC" || binary.LittleEndian.Uint16(b[4:6]) != 1 || binary.LittleEndian.Uint32(b[192:]) != crc32.Checksum(b[:192], recordCRC) {
		return nil, 0, errors.New("invalid cache record")
	}
	n := int(binary.LittleEndian.Uint16(b[6:8]))
	if n < 1 || n > 128 {
		return nil, 0, errors.New("invalid cache generation length")
	}
	e := &cacheEntry{generation: store.Generation(b[8 : 8+n]), pos: -1}
	if err := store.ValidateGeneration(e.generation); err != nil {
		return nil, 0, err
	}
	copy(e.key[:], b[136:168])
	size := int64(binary.LittleEndian.Uint64(b[168:176]))
	e.confirmed = int64(binary.LittleEndian.Uint64(b[176:184]))
	e.expires = int64(binary.LittleEndian.Uint64(b[184:192]))
	if size < 0 || e.confirmed <= 0 || e.expires <= e.confirmed {
		return nil, 0, errors.New("invalid cache size or timestamps")
	}
	return e, size, nil
}
func readCacheRecord(name string) ([]byte, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, cacheRecordSize+1))
}

func (c *metadataCache) shard(id [32]byte) *cacheShard { return &c.shards[int(id[0])&63] }

func (c *metadataCache) insert(e *cacheEntry) {
	c.setRefreshTime(e)
	sh := c.shard(e.id)
	sh.mu.Lock()
	if sh.entries == nil {
		sh.entries = make(map[[32]byte]*cacheEntry)
	}
	sh.entries[e.id] = e
	sh.mu.Unlock()
	c.entries[e.id] = e
	e.lru = c.lru.PushBack(e)
	heap.Push(&c.expiry, e)
}
func (c *metadataCache) retire(e *cacheEntry) {
	sh := c.shard(e.id)
	sh.mu.Lock()
	delete(sh.entries, e.id)
	sh.mu.Unlock()
	if c.hand == e.lru {
		c.hand = e.lru.Next()
	}
	delete(c.entries, e.id)
	c.lru.Remove(e.lru)
	heap.Remove(&c.expiry, e.pos)
	c.pending = append(c.pending, e)
}
func (c *metadataCache) pendingN() int { return len(c.pending) - c.pendingHead }
func (c *metadataCache) drain(limit int) error {
	n := min(limit, c.pendingN())
	if n <= 0 {
		return nil
	}
	if err := c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metadataEntriesBucket)
		for _, e := range c.pending[c.pendingHead : c.pendingHead+n] {
			if err := b.Delete(e.id[:]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	clear(c.pending[c.pendingHead : c.pendingHead+n])
	c.pendingHead += n
	if c.pendingN() == 0 {
		c.pending = nil
		c.pendingHead = 0
	}
	return nil
}
func (c *metadataCache) fail(err error) {
	atomic.AddUint64(&c.counters.ErrorsTotal, 1)
	if !c.bypass {
		c.cfg.Logger.Warn("chunk metadata cache bypassed until restart", "error", err)
		c.bypass = true
		c.disabled.Store(true)
	}
}
func (c *metadataCache) token(g store.Generation) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dropped[g] {
		return 0
	}
	if !c.tracked {
		return c.nextEpoch
	}
	return c.generations[g]
}
func (c *metadataCache) validToken(g store.Generation, token uint64) bool {
	if c.dropped[g] {
		return false
	}
	if !c.tracked {
		return token != 0 && token == c.nextEpoch
	}
	return token != 0 && c.generations[g] == token
}

// Hits never acquire the mutation coordinator or perform filesystem I/O.
func (c *metadataCache) lookup(id [32]byte, expected *int64) bool {
	if c.disabled.Load() {
		return false
	}
	sh := c.shard(id)
	sh.mu.RLock()
	e := sh.entries[id]
	if e == nil {
		sh.mu.RUnlock()
		return false
	}
	now := c.now()
	if now.Before(e.deadline) && (expected == nil || e.size == *expected) {
		e.visited.Store(true)
		sh.mu.RUnlock()
		if c.disabled.Load() {
			return false
		}
		c.queueRefresh(e, now)
		return true
	}
	sh.mu.RUnlock()
	c.mu.Lock()
	if c.entries[id] == e {
		c.retire(e)
	}
	c.mu.Unlock()
	return false
}

// A bounded SIEVE scan avoids a long mutation critical section when all entries
// were visited. A later fill resumes from the saved hand; dropping a fill is safe.
func (c *metadataCache) victim() *cacheEntry {
	for n := 0; n < 128; n++ {
		if c.hand == nil {
			c.hand = c.lru.Front()
		}
		if c.hand == nil {
			return nil
		}
		e := c.hand.Value.(*cacheEntry)
		c.hand = c.hand.Next()
		if !e.visited.Swap(false) {
			return e
		}
	}
	return nil
}

func (c *metadataCache) remember(g store.Generation, k store.ContentKey, size int64, token uint64, confirmed time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.bypass || size < 0 || !c.validToken(g, token) {
		return
	}
	id := c.objectID(g, k)
	// A prior miss can leave the same path in the deletion queue. Delete it
	// before publishing a replacement, otherwise a cleaner could unlink it.
	if err := c.drain(128); err != nil {
		c.fail(err)
		return
	}
	if c.pendingN() > 0 {
		atomic.AddUint64(&c.counters.FillsDroppedTotal, 1)
		return
	}
	if e := c.entries[id]; e != nil {
		c.retire(e)
		if err := c.drain(1); err != nil {
			c.fail(err)
			return
		}
	}
	if len(c.entries) >= c.cfg.MaxEntries {
		victim := c.victim()
		if victim == nil {
			atomic.AddUint64(&c.counters.FillsDroppedTotal, 1)
			return
		}
		c.retire(victim)

		if err := c.drain(1); err != nil {
			c.fail(err)
			return
		}
	}
	deadline := confirmed.Add(c.cfg.MaxTTL)
	if !c.now().Before(deadline) {
		return
	}
	e := &cacheEntry{id: id, key: k, generation: g, confirmed: confirmed.UnixNano(), expires: deadline.UnixNano(), size: size, deadline: deadline, pos: -1}
	if err := c.putRecord(e); err != nil {
		c.fail(err)
		return
	}
	c.insert(e)
}

func (c *metadataCache) setGenerations(gens []store.Generation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	next := make(map[store.Generation]uint64, len(gens))
	for _, g := range gens {
		v := c.generations[g]
		if v == 0 {
			c.nextEpoch++
			v = c.nextEpoch
		}
		next[g] = v
	}
	c.generations = next
	c.tracked = true
	for _, e := range c.entries {
		if next[e.generation] == 0 {
			c.retire(e)

		}
	}
}
func (c *metadataCache) sweep() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bypass || c.closed {
		return false
	}
	now := c.now()
	for n := 0; n < 128 && len(c.expiry) > 0 && !now.Before(c.expiry[0].deadline); n++ {
		c.retire(c.expiry[0])

	}
	if err := c.drain(128); err != nil {
		c.fail(err)
		return false
	}
	return c.pendingN() > 0 || (len(c.expiry) > 0 && !now.Before(c.expiry[0].deadline))
}
func (c *metadataCache) clean() {
	defer close(c.done)
	interval := min(c.cfg.MaxTTL/2, 30*time.Second)
	interval = max(interval, time.Millisecond)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
		}
		for c.sweep() {
			select {
			case <-c.stop:
				return
			default:
				runtime.Gosched()
			}
		}
	}
}
func (c *metadataCache) counterSnapshot() CacheCounters {
	return CacheCounters{ErrorsTotal: atomic.LoadUint64(&c.counters.ErrorsTotal), FillsDroppedTotal: atomic.LoadUint64(&c.counters.FillsDroppedTotal)}
}

func (c *metadataCache) snapshot() MetadataCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := "active"
	if c.bypass {
		state = "bypass"
	}
	if c.closed {
		state = "closed"
	}
	queued := 0
	if c.refresh != nil {
		queued = c.refresh.depth()
	}
	return MetadataCacheStats{RefreshQueueEntries: queued, CacheCounters: c.counterSnapshot(), Enabled: true, State: state, MaxTTLSeconds: c.cfg.MaxTTL.Seconds(), MaxEntries: c.cfg.MaxEntries, Entries: len(c.entries) + c.pendingN(), PendingDeleteEntries: c.pendingN(), ReloadedEntries: c.reloaded}
}
func (c *metadataCache) close() error {
	c.closeOnce.Do(func() {
		c.stopRefresh()
		close(c.stop)
		<-c.done
		c.mu.Lock()
		c.closed = true
		c.disabled.Store(true)
		c.mu.Unlock()
		c.closeErr = errors.Join(c.db.Close(), c.lock.Close())
	})
	return c.closeErr
}

// keyGates hold no historical entries; waiting is cancellable and does not hold
// the cache mutex or the S3 concurrency semaphore.
type keyGates struct {
	mu    sync.Mutex
	gates map[[32]byte]*keyGate
}
type keyGate struct {
	refs  int
	token chan struct{}
}

func (g *keyGates) acquire(ctx context.Context, id [32]byte) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	if g.gates == nil {
		g.gates = make(map[[32]byte]*keyGate)
	}
	q := g.gates[id]
	if q == nil {
		q = &keyGate{token: make(chan struct{}, 1)}
		q.token <- struct{}{}
		g.gates[id] = q
	}
	q.refs++
	g.mu.Unlock()
	drop := func() {
		g.mu.Lock()
		q.refs--
		if q.refs == 0 {
			delete(g.gates, id)
		}
		g.mu.Unlock()
	}
	select {
	case <-ctx.Done():
		drop()
		return nil, ctx.Err()
	case <-q.token:
	}
	if err := ctx.Err(); err != nil {
		q.token <- struct{}{}
		drop()
		return nil, err
	}
	return func() { q.token <- struct{}{}; drop() }, nil
}

// LockAndClearMetadataCache is for an offline purge, after stopping every Store
// sharing this storage. Hold the returned lock until the remote wipe completes.
// It never creates a cache that was not present and does not remove metrics.
func LockAndClearMetadataCache(dir, namespace string) (func() error, error) {
	if len(namespace) != 64 {
		return nil, errors.New("s3: invalid cache namespace")
	}
	if _, err := hex.DecodeString(namespace); err != nil {
		return nil, err
	}
	root := filepath.Join(dir, namespace)
	if _, err := os.Stat(root); os.IsNotExist(err) {
		return func() error { return nil }, nil
	} else if err != nil {
		return nil, err
	}
	if err := tmpfsfile.Check(root); err != nil {
		if errors.Is(err, tmpfsfile.ErrNotTmpfs) {
			return func() error { return nil }, nil
		}
		return nil, err
	}
	lock, err := tmpfsfile.Lock(filepath.Join(root, "LOCK"))
	if err != nil {
		return nil, err
	}
	for _, name := range []string{"entries", metadataDBName + ".compact", metadataDBName} {
		if err = os.RemoveAll(filepath.Join(root, name)); err != nil {
			_ = lock.Close()
			return nil, err
		}
	}
	return lock.Close, nil
}

// invalidate is synchronous for explicit backend admin operations. A full wipe
// bypasses the cache until restart; it cannot republish presence during deletion.
func (c *metadataCache) invalidate(g *store.Generation) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Never admit confirmations obtained while remote deletion is in progress.
	// Keep this generation uncached until restart, including if deletion fails.
	if g != nil {
		if c.dropped == nil {
			c.dropped = make(map[store.Generation]bool)
		}
		c.dropped[*g] = true
	}
	if g == nil {
		c.bypass = true
		c.disabled.Store(true)
	}
	c.nextEpoch++
	if c.tracked {
		if g != nil {
			if c.generations[*g] != 0 {
				c.generations[*g] = c.nextEpoch
			}
		} else {
			for generation := range c.generations {
				c.nextEpoch++
				c.generations[generation] = c.nextEpoch
			}
		}
	}
	for _, e := range c.entries {
		if g == nil || e.generation == *g {
			c.retire(e)

		}
	}
	if err := c.drain(c.pendingN()); err != nil {
		c.fail(err)
		return err
	}
	return nil
}
