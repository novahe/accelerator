package s3

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func readMetadataRecord(t *testing.T, c *metadataCache, id [32]byte) []byte {
	t.Helper()
	var value []byte
	if err := c.db.View(func(tx *bolt.Tx) error {
		value = bytes.Clone(tx.Bucket(metadataEntriesBucket).Get(id[:]))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return value
}

func overwriteMetadataRecord(t *testing.T, c *metadataCache, id [32]byte, value []byte) {
	t.Helper()
	if err := c.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(metadataEntriesBucket).Put(id[:], value)
	}); err != nil {
		t.Fatal(err)
	}
}

func persistedRecordCount(t *testing.T, c *metadataCache) int {
	t.Helper()
	count := 0
	if err := c.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(metadataEntriesBucket).ForEach(func(_, _ []byte) error { count++; return nil })
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

func writeLegacyRecord(t *testing.T, cfg Config, generation store.Generation, data []byte, confirmed time.Time) (store.ContentKey, string) {
	t.Helper()
	key := keyFromBytes(data)
	c := &metadataCache{cfg: *cfg.ChunkMetadataCache}
	id := c.objectID(generation, key)
	h := hex.EncodeToString(id[:])
	name := filepath.Join(c.cfg.Dir, c.cfg.Namespace, "entries", h[:2], h+".meta")
	e := &cacheEntry{generation: generation, key: key, confirmed: confirmed.UnixNano(), expires: confirmed.Add(c.cfg.MaxTTL).UnixNano()}
	if err := os.MkdirAll(filepath.Dir(name), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, encodeCacheRecord(e, int64(len(data))), 0o600); err != nil {
		t.Fatal(err)
	}
	return key, name
}

func TestMetadataDBImportsOnceAndRemovesLegacyInodes(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	confirmed := time.Now().Add(-time.Minute)
	key, name := writeLegacyRecord(t, cfg, "G1", []byte("migrated"), confirmed)
	_, expired := writeLegacyRecord(t, cfg, "G1", []byte("expired"), time.Now().Add(-time.Hour))
	_, corrupt := writeLegacyRecord(t, cfg, "G1", []byte("bad CRC"), confirmed)
	if err := os.WriteFile(corrupt, []byte("broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := newFakeS3()
	s := metadataStore(t, f, cfg)
	if !cacheExists(t, s, key, nil) || f.hits.head.Load() != 0 || s.MetadataCacheStats().Entries != 1 {
		t.Fatalf("migration lost positive: %+v", s.MetadataCacheStats())
	}
	if e := s.metadata.entries[s.metadata.objectID("G1", key)]; e.confirmed != confirmed.UnixNano() {
		t.Fatal("migration renewed confirmation time")
	}
	for _, path := range []string{name, expired, corrupt, filepath.Join(s.metadata.dir, "entries")} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("legacy inode retained: %s: %v", path, err)
		}
	}
	if err := s.metadata.deleteRecord(s.metadata.objectID("G1", key)); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	// A crash after import completion but before cleanup must never cause a
	// subsequently invalidated old positive to be imported again.
	writeLegacyRecord(t, cfg, "G1", []byte("migrated"), confirmed)
	next := metadataStore(t, f, cfg)
	if next.MetadataCacheStats().Entries != 0 || cacheExists(t, next, key, nil) {
		t.Fatal("completed import resurrected a deleted positive")
	}
}

func TestMetadataDBImportSkipsUnreadableIndividualRecords(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	confirmed := time.Now().Add(-time.Minute)
	good, _ := writeLegacyRecord(t, cfg, "G1", []byte("readable"), confirmed)
	_, openFailure := writeLegacyRecord(t, cfg, "G1", []byte("open failure"), confirmed)
	_, readFailure := writeLegacyRecord(t, cfg, "G1", []byte("read failure"), confirmed)
	// chmod(000) does not reject reads when tests run as root. A dangling link
	// exercises os.Open failure; a link to a directory exercises ReadAll failure.
	for _, name := range []string{openFailure, readFailure} {
		if err := os.Remove(name); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(cfg.ChunkMetadataCache.Dir, "missing"), openFailure); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(cfg.ChunkMetadataCache.Dir, readFailure); err != nil {
		t.Fatal(err)
	}
	f := newFakeS3()
	s := metadataStore(t, f, cfg)
	stats := s.MetadataCacheStats()
	if stats.Entries != 1 || stats.ErrorsTotal != 2 || stats.State != "active" || !cacheExists(t, s, good, nil) || f.hits.head.Load() != 0 {
		t.Fatalf("unreadable records blocked usable cache: %+v", stats)
	}
	if _, err := os.Stat(filepath.Join(s.metadata.dir, "entries")); !os.IsNotExist(err) {
		t.Fatal("completed import retained source files")
	}
	_ = s.Close()
	next := metadataStore(t, f, cfg)
	if next.MetadataCacheStats().Entries != 1 || next.MetadataCacheStats().ErrorsTotal != 0 || !cacheExists(t, next, good, nil) {
		t.Fatal("import errors repeated or valid record was lost on restart")
	}
}

func TestMetadataDBOneFileForAllEntriesAndTransactionRollback(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	cfg.ChunkMetadataCache.MaxEntries = 1000
	s := metadataStore(t, newFakeS3(), cfg)
	for i := 0; i < 1000; i++ {
		data := []byte{byte(i), byte(i >> 8)}
		writeS3Object(t, s, "G1", keyFromBytes(data), data, nil, true)
	}
	names, err := os.ReadDir(s.metadata.dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || persistedRecordCount(t, s.metadata) != 1000 {
		t.Fatalf("expected just LOCK and database, got %v", names)
	}
	id := s.metadata.objectID("G1", keyFromBytes([]byte{0, 0}))
	want := readMetadataRecord(t, s.metadata, id)
	rollback := errors.New("abort transaction")
	if err := s.metadata.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(metadataEntriesBucket).Delete(id[:]); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if !bytes.Equal(readMetadataRecord(t, s.metadata, id), want) {
		t.Fatal("rolled back delete was published")
	}
}

func TestMetadataDBRejectsForeignIdentity(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	s := metadataStore(t, newFakeS3(), cfg)
	if err := s.metadata.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(metadataMetaBucket).Put([]byte("namespace"), []byte("foreign"))
	}); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	if next, err := New(context.Background(), newFakeS3(), cfg); err == nil {
		_ = next.Close()
		t.Fatal("foreign database accepted")
	}
}

func TestMetadataDBInterruptedImportResumesInBatches(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	cfg.ChunkMetadataCache.MaxEntries = 1000
	s := metadataStore(t, newFakeS3(), cfg)
	dir := s.metadata.dir
	_ = s.Close()
	confirmed := time.Now().Add(-time.Minute)
	for i := 0; i < 600; i++ {
		writeLegacyRecord(t, cfg, "G1", []byte(fmt.Sprint(i)), confirmed)
	}
	db, err := openCacheDB(filepath.Join(dir, metadataDBName))
	if err != nil {
		t.Fatal(err)
	}
	// Simulate import that committed a prefix batch but not its final marker.
	if err = db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(metadataMetaBucket).Delete(metadataImportDone); err != nil {
			return err
		}
		c := &metadataCache{cfg: *cfg.ChunkMetadataCache}
		key := keyFromBytes([]byte("0"))
		id := c.objectID("G1", key)
		e := &cacheEntry{generation: "G1", key: key, confirmed: confirmed.UnixNano(), expires: confirmed.Add(c.cfg.MaxTTL).UnixNano()}
		return tx.Bucket(metadataEntriesBucket).Put(id[:], encodeCacheRecord(e, 1))
	}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	next := metadataStore(t, newFakeS3(), cfg)
	if next.MetadataCacheStats().Entries != 600 || persistedRecordCount(t, next.metadata) != 600 {
		t.Fatalf("partial import was not resumed: %+v", next.MetadataCacheStats())
	}
	if _, err := os.Stat(filepath.Join(dir, "entries")); !os.IsNotExist(err) {
		t.Fatal("resumed import retained source inodes")
	}
}

func TestMetadataDBStartupCompactionPreservesLiveRecords(t *testing.T) {
	cfg := metadataConfig(metadataDir(t))
	f := newFakeS3()
	s := metadataStore(t, f, cfg)
	data := []byte("keep through compaction")
	key := keyFromBytes(data)
	writeS3Object(t, s, "G1", key, data, nil, true)
	id := s.metadata.objectID("G1", key)
	want := readMetadataRecord(t, s.metadata, id)
	// Oversized malformed values simulate a database whose old contents will
	// be discarded during recovery. This also makes fragmentation deterministic.
	if err := s.metadata.db.Update(func(tx *bolt.Tx) error {
		for i := 0; i < 18; i++ {
			if err := tx.Bucket(metadataEntriesBucket).Put([]byte(fmt.Sprint(i)), make([]byte, 1<<20)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(s.metadata.dir, metadataDBName)
	before, err := os.Stat(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	if err = os.WriteFile(dbPath+".compact", []byte("interrupted copy"), 0o600); err != nil {
		t.Fatal(err)
	}
	next := metadataStore(t, f, cfg)
	after, err := os.Stat(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size()/2 || persistedRecordCount(t, next.metadata) != 1 ||
		!bytes.Equal(readMetadataRecord(t, next.metadata, id), want) || !cacheExists(t, next, key, nil) || f.hits.head.Load() != 1 {
		t.Fatalf("compaction lost content or retained free space: %d -> %d", before.Size(), after.Size())
	}
	if _, err = os.Stat(dbPath + ".compact"); !os.IsNotExist(err) {
		t.Fatal("compaction residue retained")
	}
}

// This subprocess exits inside a write transaction, without a deferred Close
// or Rollback. A separate process must still recover committed records only.
func TestMetadataDBProcessExitDuringTransaction(t *testing.T) {
	if root := os.Getenv("STORE_METADATA_CRASH_TEST_DIR"); root != "" {
		cfg := metadataConfig(root)
		s, err := New(context.Background(), newFakeS3(), cfg)
		if err != nil {
			os.Exit(2)
		}
		id := s.metadata.objectID("G1", keyFromBytes([]byte("committed")))
		if err := s.metadata.db.Update(func(tx *bolt.Tx) error {
			if err := tx.Bucket(metadataEntriesBucket).Delete(id[:]); err != nil {
				return err
			}
			os.Exit(23)
			return nil
		}); err != nil {
			os.Exit(3)
		}
		os.Exit(4)
	}
	cfg := metadataConfig(metadataDir(t))
	f := newFakeS3()
	s := metadataStore(t, f, cfg)
	data := []byte("committed")
	key := keyFromBytes(data)
	writeS3Object(t, s, "G1", key, data, nil, true)
	_ = s.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestMetadataDBProcessExitDuringTransaction$")
	cmd.Env = append(os.Environ(), "STORE_METADATA_CRASH_TEST_DIR="+cfg.ChunkMetadataCache.Dir)
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("crash subprocess returned successfully: %s", out)
	} else if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 23 {
		t.Fatalf("unexpected crash: %s: %v", out, err)
	}
	next := metadataStore(t, f, cfg)
	if next.MetadataCacheStats().Entries != 1 || !cacheExists(t, next, key, nil) || f.hits.head.Load() != 1 {
		t.Fatal("uncommitted transaction destroyed committed record")
	}
}

func TestMetadataDBSIGKILLDuringNoSyncWrites(t *testing.T) {
	if root := os.Getenv("STORE_METADATA_KILL_TEST_DIR"); root != "" {
		cfg := metadataConfig(root)
		cfg.ChunkMetadataCache.MaxEntries = 100
		s, err := New(context.Background(), newFakeS3(), cfg)
		if err != nil {
			os.Exit(2)
		}
		for i := 0; ; i++ {
			key := keyFromBytes([]byte(fmt.Sprint(i % 100)))
			confirmed := time.Now()
			e := &cacheEntry{id: s.metadata.objectID("G1", key), generation: "G1", key: key,
				size: 1, confirmed: confirmed.UnixNano(), expires: confirmed.Add(time.Minute).UnixNano()}
			if err := s.metadata.putRecord(e); err != nil {
				os.Exit(3)
			}
			if i == 99 {
				fmt.Fprintln(os.Stdout, "committed")
			}
		}
	}
	cfg := metadataConfig(metadataDir(t))
	cfg.ChunkMetadataCache.MaxEntries = 100
	cmd := exec.Command(os.Args[0], "-test.run=^TestMetadataDBSIGKILLDuringNoSyncWrites$")
	cmd.Env = append(os.Environ(), "STORE_METADATA_KILL_TEST_DIR="+cfg.ChunkMetadataCache.Dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	ready := make(chan error, 1)
	go func() {
		line, err := bufio.NewReader(stdout).ReadString('\n')
		if err == nil && line != "committed\n" {
			err = fmt.Errorf("unexpected child output: %q", line)
		}
		ready <- err
	}()
	select {
	case err = <-ready:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not commit initial records")
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("writer exited normally")
	}
	next := metadataStore(t, newFakeS3(), cfg)
	if next.MetadataCacheStats().Entries != 100 || persistedRecordCount(t, next.metadata) != 100 {
		t.Fatalf("SIGKILL lost committed records: %+v, child: %s", next.MetadataCacheStats(), stderr.String())
	}
	for i := 0; i < 100; i++ {
		if !cacheExists(t, next, keyFromBytes([]byte(fmt.Sprint(i))), nil) {
			t.Fatalf("SIGKILL lost key %d", i)
		}
	}
}
