//go:build linux

package tmpfsfile

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func check(f *os.File) error {
	var st unix.Statfs_t
	if err := unix.Fstatfs(int(f.Fd()), &st); err != nil {
		return err
	}
	if st.Type != unix.TMPFS_MAGIC {
		return fmt.Errorf("tmpfs: %s: %w", f.Name(), ErrNotTmpfs)
	}
	return nil
}

func lock(f *os.File) error { return unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB) }
