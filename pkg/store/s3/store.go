package s3

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/util/optrace"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

const defaultMaxObjectSize = 16 << 20

var (
	ErrKeyMismatch       = errors.New("s3: content key mismatch")
	ErrCommitKeyMismatch = errors.New("s3: commit key differs from open key")
)

// Config controls only the S3 object data plane. Generation list metadata is
// configured and loaded independently by store-ctl.
type Config struct {
	Bucket             string
	Prefix             string
	MaxInflight        int
	OpTimeout          time.Duration
	MaxObjectSize      int64
	ChunkMetadataCache *ChunkMetadataCacheConfig
}

// Store operates on exactly the generation supplied to each method.
type Store struct {
	client     s3Client
	prefix     string
	maxSize    int64
	timeout    time.Duration
	sem        chan struct{}
	tr         *optrace.Tracer
	metadata   *metadataCache
	gates      keyGates
	chunkHeads atomic.Uint64
	chunkPuts  atomic.Uint64
}

// New constructs a data-plane Store without loading generation metadata. The
// context parameter is retained for source compatibility and is not stored.
func New(_ context.Context, client s3Client, cfg Config) (*Store, error) {
	if client == nil {
		return nil, errors.New("s3: nil s3 client")
	}
	if cfg.Bucket == "" {
		return nil, errors.New("s3: bucket is required")
	}
	if cfg.ChunkMetadataCache != nil {
		if cfg.OpTimeout < 0 {
			return nil, errors.New("s3: chunk metadata cache requires positive op timeout")
		}
		if cfg.OpTimeout == 0 {
			cfg.OpTimeout = 10 * time.Second
		}
	}
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = 64
	}
	if cfg.MaxObjectSize <= 0 {
		cfg.MaxObjectSize = defaultMaxObjectSize
	}
	s := &Store{
		client:  client,
		prefix:  normalisePrefix(cfg.Prefix),
		maxSize: cfg.MaxObjectSize,
		timeout: cfg.OpTimeout,
		sem:     make(chan struct{}, cfg.MaxInflight),
		tr:      optrace.FromEnv("store-ctl"),
	}
	if cfg.ChunkMetadataCache != nil {
		var err error
		s.metadata, err = newMetadataCache(*cfg.ChunkMetadataCache)
		if err != nil {
			return nil, err
		}
	}
	if s.metadata != nil && s.metadata.refresh != nil {
		s.metadata.refresh.start(s)
	}
	return s, nil
}

func (s *Store) Get(ctx context.Context, generation store.Generation, partition store.Partition, key store.ContentKey) (bool, []byte, error) {
	if err := store.ValidateGeneration(generation); err != nil {
		return false, nil, fmt.Errorf("s3: %w", err)
	}
	body, _, err := s.boundedGet(ctx, func(ctx context.Context) ([]byte, *ObjectMeta, error) {
		return s.client.Get(ctx, s.objectKey(partition, generation, key))
	})
	if errors.Is(err, ErrNotFound) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, fmt.Errorf("s3: get generation=%s: %w", generation, err)
	}
	return true, body, nil
}

func (s *Store) Exists(ctx context.Context, generation store.Generation, partition store.Partition, key store.ContentKey, expectedSize *int64) (bool, error) {
	exists, _, err := s.ExistsWithSource(ctx, generation, partition, key, expectedSize)
	return exists, err
}

// ExistsWithSource lets the RPC layer distinguish cache decisions from HEAD
// decisions without relying on a racy before/after counter difference.
func (s *Store) ExistsWithSource(ctx context.Context, generation store.Generation, partition store.Partition, key store.ContentKey, expectedSize *int64) (bool, bool, error) {
	if err := store.ValidateGeneration(generation); err != nil {
		return false, false, fmt.Errorf("s3: %w", err)
	}
	if expectedSize != nil && *expectedSize < 0 {
		return false, false, fmt.Errorf("s3: negative expected size %d", *expectedSize)
	}
	ctx, cancel, id, hit, err := s.chunkStage(ctx, generation, partition, key, expectedSize)
	defer cancel()
	if err != nil || hit {
		return hit, hit, err
	}
	unlock, err := s.objectGate(ctx, partition, id)
	if err != nil {
		return false, false, err
	}
	defer unlock()
	return s.existsLocked(ctx, generation, partition, key, expectedSize, id)
}

// chunkStage avoids timers and gates on hits. Misses retain the deadline that
// started before lookup, including its lock wait and subsequent gate/remote work.
func (s *Store) chunkStage(parent context.Context, g store.Generation, p store.Partition, k store.ContentKey, expected *int64) (context.Context, context.CancelFunc, [32]byte, bool, error) {
	noop := func() {}
	var id [32]byte
	if err := parent.Err(); err != nil {
		return parent, noop, id, false, err
	}
	if s.metadata == nil || p != store.PartitionChunk {
		return parent, noop, id, false, nil
	}
	deadline := time.Now().Add(s.timeout)
	if d, ok := parent.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	id = s.metadata.objectID(g, k)
	hit := s.metadata.lookup(id, expected)
	if err := parent.Err(); err != nil {
		return parent, noop, id, false, err
	}
	if !time.Now().Before(deadline) {
		return parent, noop, id, false, context.DeadlineExceeded
	}
	if hit {
		return parent, noop, id, true, nil
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	return ctx, cancel, id, false, nil
}

func (s *Store) objectGate(ctx context.Context, p store.Partition, id [32]byte) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.metadata == nil || p != store.PartitionChunk {
		return func() {}, nil
	}
	return s.gates.acquire(ctx, id)
}

func (s *Store) existsLocked(ctx context.Context, g store.Generation, p store.Partition, k store.ContentKey, expected *int64, id [32]byte) (bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return false, false, err
	}
	cached := s.metadata != nil && p == store.PartitionChunk
	if cached && s.metadata.lookup(id, expected) {
		return true, true, nil
	}
	var token uint64
	if cached {
		token = s.metadata.token(g)
	}
	meta, err := s.boundedHead(ctx, func(ctx context.Context) (*ObjectMeta, error) {
		if p == store.PartitionChunk {
			s.chunkHeads.Add(1)
		}
		return s.client.Head(ctx, s.objectKey(p, g, k))
	})
	if errors.Is(err, ErrNotFound) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("s3: head generation=%s: %w", g, err)
	}
	if meta == nil {
		return false, false, fmt.Errorf("s3: head generation=%s returned no metadata", g)
	}
	if expected != nil && meta.Size != *expected {
		return false, false, nil
	}
	if cached {
		s.metadata.remember(g, k, meta.Size, token, time.Now())
	}
	return true, false, nil
}

// UpdateGenerations invalidates retired generations without waiting for S3 or
// object gates. Old in-flight confirmations cannot repopulate their records.
func (s *Store) UpdateGenerations(gens []store.Generation) {
	if s.metadata != nil {
		s.metadata.setGenerations(gens)
	}
}
func (s *Store) MetadataCacheStats() MetadataCacheStats {
	if s.metadata == nil {
		return MetadataCacheStats{State: "disabled"}
	}
	return s.metadata.snapshot()
}
func (s *Store) ChunkIOStats() (heads, puts uint64) { return s.chunkHeads.Load(), s.chunkPuts.Load() }

// StopBackground joins the refresh worker before a final metrics snapshot.
func (s *Store) StopBackground() {
	if s.metadata != nil {
		s.metadata.stopRefresh()
	}
}

// Close preserves unexpired records for the next process. The caller owns the SDK client.
func (s *Store) Close() error {
	if s.metadata != nil {
		return s.metadata.close()
	}
	return nil
}

func (s *Store) OpenPut(generation store.Generation, partition store.Partition, key store.ContentKey, expectedSize *int64) (store.PutHandle, error) {
	if err := store.ValidateGeneration(generation); err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	h := &putHandle{
		store:      s,
		generation: generation,
		partition:  partition,
		key:        key,
		ctx:        context.Background(),
	}
	if expectedSize != nil {
		if *expectedSize < 0 {
			return nil, fmt.Errorf("s3: negative expected size %d", *expectedSize)
		}
		h.hasExpected = true
		h.expected = *expectedSize
	}
	return h, nil
}

func (s *Store) objectKey(partition store.Partition, generation store.Generation, key store.ContentKey) string {
	hexKey := hex.EncodeToString(key[:])
	return path.Join(s.prefix, string(partition), string(generation), hexKey[:2], hexKey[2:4], hexKey)
}

func (s *Store) opContext(parent context.Context) (context.Context, context.CancelFunc) {
	if s.timeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, s.timeout)
}

func (s *Store) boundedGet(parent context.Context, fn func(context.Context) ([]byte, *ObjectMeta, error)) ([]byte, *ObjectMeta, error) {
	select {
	case s.sem <- struct{}{}:
	case <-parent.Done():
		return nil, nil, parent.Err()
	}
	defer func() { <-s.sem }()
	defer s.tr.Begin("s3.get")()
	ctx, cancel := s.opContext(parent)
	defer cancel()
	body, meta, err := fn(ctx)
	if err == nil && meta != nil && meta.Size > s.maxSize {
		return nil, nil, fmt.Errorf("s3: object size %d exceeds max %d", meta.Size, s.maxSize)
	}
	return body, meta, err
}

func (s *Store) boundedHead(parent context.Context, fn func(context.Context) (*ObjectMeta, error)) (*ObjectMeta, error) {
	select {
	case s.sem <- struct{}{}:
	case <-parent.Done():
		return nil, parent.Err()
	}
	defer func() { <-s.sem }()
	defer s.tr.Begin("s3.head")()
	ctx, cancel := s.opContext(parent)
	defer cancel()
	return fn(ctx)
}

func (s *Store) boundedPut(parent context.Context, fn func(context.Context) (string, error)) (string, error) {
	select {
	case s.sem <- struct{}{}:
	case <-parent.Done():
		return "", parent.Err()
	}
	defer func() { <-s.sem }()
	defer s.tr.Begin("s3.put")()
	ctx, cancel := s.opContext(parent)
	defer cancel()
	return fn(ctx)
}

func normalisePrefix(prefix string) string { return strings.Trim(prefix, "/") }
