package s3

import (
	"container/heap"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/util/chunkmeta"
	"github.com/kuasar-sandbox/accelerator/internal/util/tmpfsfile"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// ChunkRefreshConfig enables access-triggered verification. There is exactly
// one worker; nil disables it for raw Stores. The application enables it by default.
type ChunkRefreshConfig struct {
	MaxQPS float64
}

type metadataRefresh struct {
	c        *metadataCache
	seed     [32]byte
	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	once     sync.Once
	mu       sync.Mutex
	stopped  bool
	jobs     []*cacheEntry
	head     int
	wake     chan struct{}
	// Owned only by the single worker.
	nextStart time.Time
	failures  uint
}

func refreshInterval(qps float64) (time.Duration, error) {
	return chunkmeta.RefreshInterval(qps)
}

func newMetadataRefresh(c *metadataCache, qps float64) (*metadataRefresh, error) {
	interval, err := refreshInterval(qps)
	if err != nil {
		return nil, err
	}
	r := &metadataRefresh{c: c, interval: interval, wake: make(chan struct{}, 1)}
	name := filepath.Join(c.dir, "REFRESH_SEED")
	f, err := os.Open(name)
	if errors.Is(err, os.ErrNotExist) {
		if _, err = rand.Read(r.seed[:]); err == nil {
			err = tmpfsfile.Write(name, r.seed[:])
		}
	} else if err == nil {
		var data []byte
		data, err = io.ReadAll(io.LimitReader(f, 33))
		_ = f.Close()
		if err == nil && len(data) != len(r.seed) {
			err = errors.New("invalid seed length")
		}
		copy(r.seed[:], data)
	}
	if err != nil {
		return nil, fmt.Errorf("s3 refresh seed: %w", err)
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	return r, nil
}

func (c *metadataCache) setRefreshTime(e *cacheEntry) {
	if c.refresh == nil {
		return
	}
	var input [72]byte
	copy(input[:32], c.refresh.seed[:])
	copy(input[32:64], e.id[:])
	binary.LittleEndian.PutUint64(input[64:], uint64(e.confirmed))
	hash := sha256.Sum256(input[:])
	lifetime := time.Duration(e.expires - e.confirmed)
	lower, span := lifetime-lifetime/4, lifetime/8
	jitter := time.Duration(binary.LittleEndian.Uint64(hash[:8]) % uint64(span+1))
	e.refreshAt = e.deadline.Add(-lifetime + lower + jitter)
}

// Only a hit in the soft window touches the queue. No timer scans the cache.
func (c *metadataCache) queueRefresh(e *cacheEntry, now time.Time) {
	r := c.refresh
	if r == nil || now.Sub(e.refreshAt) < time.Duration(e.retryAfter.Load()) || !e.refreshPending.CompareAndSwap(false, true) {
		return
	}
	r.mu.Lock()
	if r.stopped || len(r.jobs)-r.head >= c.cfg.MaxEntries {
		r.mu.Unlock()
		e.refreshPending.Store(false)
		return // A subsequent access may retry; the hard deadline never moves.
	}
	r.jobs = append(r.jobs, e)
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *metadataRefresh) depth() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.jobs) - r.head
}

func (r *metadataRefresh) take() *cacheEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.head == len(r.jobs) {
		return nil
	}
	e := r.jobs[r.head]
	r.jobs[r.head] = nil
	r.head++
	if r.head == len(r.jobs) {
		r.jobs, r.head = nil, 0
	} else if r.head >= 4096 && r.head >= len(r.jobs)/2 {
		n := copy(r.jobs, r.jobs[r.head:])
		clear(r.jobs[n:])
		r.jobs, r.head = r.jobs[:n], 0
	}
	return e
}

func (r *metadataRefresh) start(s *Store) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for r.ctx.Err() == nil {
			e := r.take()
			if e == nil {
				select {
				case <-r.ctx.Done():
					return
				case <-r.wake:
				}
				continue
			}
			s.refreshChunk(r, e)
			e.refreshPending.Store(false)
		}
	}()
}

func (c *metadataCache) stopRefresh() {
	if r := c.refresh; r != nil {
		r.once.Do(func() {
			r.mu.Lock()
			r.stopped = true
			r.cancel()
			r.mu.Unlock()
			r.wg.Wait()
			r.mu.Lock()
			for _, e := range r.jobs[r.head:] {
				e.refreshPending.Store(false)
			}
			r.jobs, r.head = nil, 0
			r.mu.Unlock()
		})
	}
}

func (c *metadataCache) refreshToken(e *cacheEntry) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.bypass || c.entries[e.id] != e || !c.now().Before(e.deadline) || c.dropped[e.generation] {
		return 0
	}
	if !c.tracked {
		return c.nextEpoch
	}
	return c.generations[e.generation]
}

func waitRefresh(ctx context.Context, until time.Time) error {
	if delay := time.Until(until); delay > 0 {
		t := time.NewTimer(delay)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	return ctx.Err()
}

func (r *metadataRefresh) failed(e *cacheEntry, err error) {
	if r.ctx.Err() != nil {
		return
	}
	atomic.AddUint64(&r.c.counters.ErrorsTotal, 1)
	r.failures = min(r.failures+1, 6)
	delay := time.Second << (r.failures - 1)
	delay += delay / 4 * time.Duration(e.id[0]) / 255
	r.nextStart = time.Now().Add(max(r.interval, delay))
	e.retryAfter.Store(int64(r.c.now().Sub(e.refreshAt) + delay))
	r.c.cfg.Logger.Warn("chunk metadata verification failed; keeping original deadline", "error", err)
}

func (s *Store) refreshChunk(r *metadataRefresh, e *cacheEntry) {
	c := r.c
	token := c.refreshToken(e)
	if token == 0 {
		return
	}
	// Queue/rate waits cannot cross the old hard deadline. The operation budget
	// starts when rate scheduling finishes and covers gate, semaphore and HEAD.
	waitCtx, stop := context.WithDeadline(r.ctx, e.deadline)
	defer stop()
	if waitRefresh(waitCtx, r.nextStart) != nil {
		return
	}
	ctx, cancel := context.WithTimeout(waitCtx, s.timeout)
	defer cancel()
	unlock, err := s.gates.acquire(ctx, e.id)
	if err != nil {
		r.failed(e, err)
		return
	}
	defer unlock()
	if c.refreshToken(e) != token {
		return
	}
	meta, err := s.boundedHead(ctx, func(ctx context.Context) (*ObjectMeta, error) {
		r.nextStart = time.Now().Add(r.interval)
		s.chunkHeads.Add(1)
		return s.client.Head(ctx, s.objectKey(store.PartitionChunk, e.generation, e.key))
	})
	confirmed := c.now()
	if ctx.Err() != nil {
		r.failed(e, ctx.Err())
		return
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		r.failed(e, err)
		return
	}
	if err == nil && (meta == nil || meta.Size < 0) {
		r.failed(e, errors.New("HEAD returned invalid metadata"))
		return
	}
	r.failures = 0
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.bypass || c.entries[e.id] != e || !c.validToken(e.generation, token) || !c.now().Before(e.deadline) {
		return
	}
	if errors.Is(err, ErrNotFound) || meta.Size != e.size {
		// Invalidate persistent presence before publishing the in-memory miss.
		// Leaving this to the periodic cleaner can resurrect a known-invalid
		// positive when the process restarts before pending deletes are drained.
		removeErr := c.deleteRecord(e.id)
		c.retire(e)
		if removeErr != nil {
			c.fail(removeErr)
		}
		return
	}
	deadline := confirmed.Add(c.cfg.MaxTTL)
	next := &cacheEntry{id: e.id, key: e.key, generation: e.generation, size: e.size,
		confirmed: confirmed.UnixNano(), expires: deadline.UnixNano(), deadline: deadline,
		pos: e.pos, lru: e.lru}
	c.setRefreshTime(next)
	if err := c.putRecord(next); err != nil {
		c.fail(err)
		return
	}
	// Keep eviction position/visited state. Refreshing is not a foreground hit.
	sh := c.shard(e.id)
	sh.mu.Lock()
	next.visited.Store(e.visited.Load())
	sh.entries[e.id] = next
	sh.mu.Unlock()
	c.entries[e.id] = next
	next.lru.Value = next
	c.expiry[next.pos] = next
	heap.Fix(&c.expiry, next.pos)
}
