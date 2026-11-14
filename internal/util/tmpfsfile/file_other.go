//go:build !linux

package tmpfsfile

import (
	"errors"
	"os"
)

func check(*os.File) error { return errors.New("tmpfs files require Linux") }
func lock(*os.File) error  { return errors.New("tmpfs files require Linux") }
