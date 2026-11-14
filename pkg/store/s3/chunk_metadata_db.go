package s3

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
)

const metadataDBName = "metadata.db"

var (
	metadataEntriesBucket = []byte("entries")
	metadataMetaBucket    = []byte("meta")
	metadataImportDone    = []byte("legacy-import-complete")
)

func openCacheDB(name string) (*bolt.DB, error) {
	// This disposable database is restricted to tmpfs. Skip explicit syncing
	// on commits and mmap growth; writes still complete before publishing the
	// memory entry. A finite lock wait protects against external DB tools.
	return bolt.Open(name, 0o600, &bolt.Options{
		Timeout: time.Second, FreelistType: bolt.FreelistMapType,
		NoSync: true, NoGrowSync: true,
	})
}

func (c *metadataCache) openDB() error {
	name := filepath.Join(c.dir, metadataDBName)
	// The namespace LOCK is already held. An interrupted compaction never
	// replaces its source until the new database is complete and closed.
	if err := os.Remove(name + ".compact"); err != nil && !os.IsNotExist(err) {
		return err
	}
	var err error
	if c.db, err = openCacheDB(name); err != nil {
		return err
	}
	if err = c.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket(metadataMetaBucket)
		if meta != nil {
			if !bytes.Equal(meta.Get([]byte("format")), []byte("1")) ||
				!bytes.Equal(meta.Get([]byte("namespace")), []byte(c.cfg.Namespace)) ||
				tx.Bucket(metadataEntriesBucket) == nil {
				return errors.New("s3: invalid metadata database identity or format")
			}
			return nil
		}
		if tx.Bucket(metadataEntriesBucket) != nil {
			return errors.New("s3: metadata database has no identity")
		}
		meta, err := tx.CreateBucket(metadataMetaBucket)
		if err != nil {
			return err
		}
		if _, err = tx.CreateBucket(metadataEntriesBucket); err != nil {
			return err
		}
		if err = meta.Put([]byte("format"), []byte("1")); err != nil {
			return err
		}
		return meta.Put([]byte("namespace"), []byte(c.cfg.Namespace))
	}); err != nil {
		return err
	}
	return c.importLegacyRecords()
}

type metadataMutation struct {
	key, value []byte // nil value means delete
}

func (c *metadataCache) applyRecords(changes []metadataMutation) error {
	if len(changes) == 0 {
		return nil
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metadataEntriesBucket)
		for _, change := range changes {
			var err error
			if change.value == nil {
				err = b.Delete(change.key)
			} else {
				err = b.Put(change.key, change.value)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *metadataCache) putRecord(e *cacheEntry) error {
	return c.applyRecords([]metadataMutation{{key: e.id[:], value: encodeCacheRecord(e, e.size)}})
}

func (c *metadataCache) deleteRecord(id [32]byte) error {
	return c.applyRecords([]metadataMutation{{key: id[:]}})
}

// decodeOwnedRecord copies every field out of the transaction's mmap bytes.
// Configuration changes may shorten a deadline, but never renew a record.
func (c *metadataCache) decodeOwnedRecord(key, value []byte, now time.Time) (*cacheEntry, error) {
	e, size, err := decodeCacheRecord(value)
	if err != nil {
		return nil, err
	}
	e.id = c.objectID(e.generation, e.key)
	e.expires = min(e.expires, time.Unix(0, e.confirmed).Add(c.cfg.MaxTTL).UnixNano())
	if !bytes.Equal(key, e.id[:]) || e.confirmed > now.UnixNano() || e.expires <= now.UnixNano() {
		return nil, errors.New("expired or misplaced metadata record")
	}
	e.size = size
	e.deadline = now.Add(time.Duration(e.expires - now.UnixNano()))
	return e, nil
}

func (c *metadataCache) reload() error {
	now := c.now()
	var changes []metadataMutation
	// Copy mutations and apply after closing the read transaction. Opening a
	// writer while holding a reader can deadlock bbolt's mmap growth.
	if err := c.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(metadataEntriesBucket).ForEach(func(key, value []byte) error {
			e, err := c.decodeOwnedRecord(key, value, now)
			if err != nil {
				changes = append(changes, metadataMutation{key: bytes.Clone(key)})
				return nil
			}
			if len(c.entries) >= c.cfg.MaxEntries {
				// Prefer the newest confirmations when a restart lowers capacity.
				if !c.expiry[0].deadline.Before(e.deadline) {
					changes = append(changes, metadataMutation{key: bytes.Clone(key)})
					return nil
				}
				c.retire(c.expiry[0])
			}
			if int64(binary.LittleEndian.Uint64(value[184:192])) != e.expires {
				changes = append(changes, metadataMutation{key: bytes.Clone(key), value: encodeCacheRecord(e, e.size)})
			}
			c.insert(e)
			return nil
		})
	}); err != nil {
		return err
	}
	if err := c.applyRecords(changes); err != nil {
		return err
	}
	return c.drain(c.pendingN())
}

// Import is one-time, restartable, and never dual-writes. Keep source files
// until the completion marker commits. After that marker, any leftover source
// files are only garbage: re-importing them could resurrect an invalidated key.
func (c *metadataCache) importLegacyRecords() error {
	var done bool
	if err := c.db.View(func(tx *bolt.Tx) error {
		done = bytes.Equal(tx.Bucket(metadataMetaBucket).Get(metadataImportDone), []byte{1})
		return nil
	}); err != nil {
		return err
	}
	root := filepath.Join(c.dir, "entries")
	if done {
		return os.RemoveAll(root)
	}
	now := c.now()
	var batch []metadataMutation
	err := filepath.WalkDir(root, func(name string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if name == root && os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".meta") {
			return nil
		}
		id, err := hex.DecodeString(strings.TrimSuffix(d.Name(), ".meta"))
		if err != nil || len(id) != 32 || name != filepath.Join(root, d.Name()[:2], d.Name()) {
			return nil
		}
		raw, err := readCacheRecord(name)
		if err != nil {
			// An individual disposable record must not prevent the service
			// starting. Directory traversal and database errors still fail startup.
			atomic.AddUint64(&c.counters.ErrorsTotal, 1)
			c.cfg.Logger.Warn("chunk metadata import skipped unreadable record", "path", name, "error", err)
			return nil
		}
		e, err := c.decodeOwnedRecord(id, raw, now)
		if err != nil {
			return nil
		}
		batch = append(batch, metadataMutation{key: e.id[:], value: encodeCacheRecord(e, e.size)})
		if len(batch) < 512 {
			return nil
		}
		err = c.applyRecords(batch)
		batch = nil
		return err
	})
	if err != nil {
		return fmt.Errorf("import metadata records: %w", err)
	}
	if err = c.applyRecords(batch); err != nil {
		return err
	}
	if err = c.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(metadataMetaBucket).Put(metadataImportDone, []byte{1})
	}); err != nil {
		return err
	}
	return os.RemoveAll(root)
}

func (c *metadataCache) compactAtStartup() error {
	var size int64
	if err := c.db.View(func(tx *bolt.Tx) error { size = tx.Size(); return nil }); err != nil {
		return err
	}
	stats := c.db.Stats()
	free := int64(stats.FreePageN+stats.PendingPageN) * int64(c.db.Info().PageSize)
	if size < 16<<20 || free < size/2 {
		return nil
	}
	name := filepath.Join(c.dir, metadataDBName)
	temp := name + ".compact"
	defer os.Remove(temp)
	dst, err := openCacheDB(temp)
	if err != nil {
		return err
	}
	err = bolt.Compact(dst, c.db, 1<<20)
	if err = errors.Join(err, dst.Close()); err != nil {
		return err
	}
	if err = c.db.Close(); err != nil {
		return err
	}
	if err = os.Rename(temp, name); err != nil {
		return err
	}
	c.db, err = openCacheDB(name)
	return err
}
