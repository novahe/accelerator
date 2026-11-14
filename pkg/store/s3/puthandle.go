package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type putHandle struct {
	store       *Store
	generation  store.Generation
	partition   store.Partition
	key         store.ContentKey
	hasExpected bool
	expected    int64
	ctx         context.Context
	buf         bytes.Buffer
	terminated  bool
	cacheHit    bool
}

// SetContext lets the gRPC server bind backend work to the stream context
// without widening the shared PutHandle interface.
func (h *putHandle) SetContext(ctx context.Context) {
	if ctx != nil && !h.terminated {
		h.ctx = ctx
	}
}

func (h *putHandle) Write(p []byte) (int, error) {
	if h.terminated {
		return 0, errors.New("s3: write after commit/abort")
	}
	if int64(len(p)) > h.store.maxSize-int64(h.buf.Len()) {
		return 0, fmt.Errorf("s3: payload exceeds max object size %d", h.store.maxSize)
	}
	if h.hasExpected && int64(len(p)) > h.expected-int64(h.buf.Len()) {
		return 0, fmt.Errorf("s3: payload exceeds expected size %d", h.expected)
	}
	return h.buf.Write(p)
}

func (h *putHandle) Commit(key store.ContentKey, verifyDigest store.ContentKey) (bool, error) {
	if h.terminated {
		return false, errors.New("s3: commit on terminated handle")
	}
	h.terminated = true
	defer h.buf.Reset()
	if key != h.key {
		return false, ErrCommitKeyMismatch
	}
	if h.hasExpected && int64(h.buf.Len()) != h.expected {
		return false, fmt.Errorf("s3: payload size %d, expected %d", h.buf.Len(), h.expected)
	}
	if verifyDigest != h.key {
		return false, ErrKeyMismatch
	}

	expected := (*int64)(nil)
	if h.hasExpected {
		value := h.expected
		expected = &value
	}
	ctx, cancel, id, hit, err := h.store.chunkStage(h.ctx, h.generation, h.partition, h.key, expected)
	defer cancel()
	if err != nil {
		return false, err
	}
	if hit {
		h.cacheHit = true
		return false, nil
	}
	unlock, err := h.store.objectGate(ctx, h.partition, id)
	if err != nil {
		return false, err
	}
	defer unlock()
	var token uint64
	if h.store.metadata != nil && h.partition == store.PartitionChunk {
		token = h.store.metadata.token(h.generation)
	}
	exists, cacheHit, err := h.store.existsLocked(ctx, h.generation, h.partition, h.key, expected, id)
	if err != nil {
		return false, err
	}
	if exists {
		h.cacheHit = cacheHit
		return false, nil
	}

	objectKey := h.store.objectKey(h.partition, h.generation, h.key)
	body := append([]byte(nil), h.buf.Bytes()...)
	if _, err := h.store.boundedPut(ctx, func(ctx context.Context) (string, error) {
		if h.partition == store.PartitionChunk {
			h.store.chunkPuts.Add(1)
		}
		return h.store.client.Put(ctx, objectKey, body, PutOptions{ContentType: "application/octet-stream"})
	}); err != nil {
		return false, fmt.Errorf("s3: put %s: %w", objectKey, err)
	}
	if h.store.metadata != nil && h.partition == store.PartitionChunk {
		h.store.metadata.remember(h.generation, h.key, int64(len(body)), token, time.Now())
	}
	return true, nil
}

func (h *putHandle) MetadataCacheHit() bool { return h.cacheHit }

func (h *putHandle) Abort() error {
	if h.terminated {
		return nil
	}
	h.terminated = true
	h.buf.Reset()
	return nil
}
