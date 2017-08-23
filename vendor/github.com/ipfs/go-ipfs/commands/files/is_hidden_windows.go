// +build windows

package files

import (
	"path/filepath"
	"strings"
	"syscall"
)

func IsHidden(f File) bool {

	fName := filepath.Base(f.FileName())

	if strings.HasPrefix(fName, ".") && len(fName) > 1 {
		return true
	}

	p, e := syscall.UTF16PtrFromString(f.FullPath())
	if e != nil {
		return false
	}

	attrs, e := syscall.GetFileAttributes(p)
	if e != nil {
		return false
	}
	return attrs&syscall.FILE_ATTRIBUTE_HIDDEN != 0
}
