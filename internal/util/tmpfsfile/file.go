// Package tmpfsfile owns disposable, atomically published files on tmpfs.
package tmpfsfile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrNotTmpfs distinguishes a non-cache residue from a permission/I/O error.
var ErrNotTmpfs = errors.New("not on tmpfs")

// Check examines an existing directory without creating anything.
func Check(dir string) error {
	if !filepath.IsAbs(dir) {
		return fmt.Errorf("tmpfs: directory must be absolute: %s", dir)
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return check(f)
}

// Dir validates the existing ancestor before creating missing directories, then
// rechecks the resulting directory. A rejected non-tmpfs path leaves no debris.
func Dir(dir string) error {
	if !filepath.IsAbs(dir) {
		return fmt.Errorf("tmpfs: directory must be absolute: %s", dir)
	}
	ancestor := dir
	for {
		_, err := os.Stat(ancestor)
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			return err
		}
		ancestor = filepath.Dir(ancestor)
	}
	if err := Check(ancestor); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return Check(dir)
}

// Lock must be held until all users, including publishers, have stopped.
// The lock inode is deliberately never unlinked or replaced.
func Lock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err = lock(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("tmpfs: lock %s: %w", path, err)
	}
	return f, nil
}

// Write publishes a complete snapshot. It intentionally does not fsync tmpfs.
func Write(path string, body []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(body); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
