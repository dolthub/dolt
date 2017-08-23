// +build !windows

package files

import (
	"path/filepath"
	"strings"
)

func IsHidden(f File) bool {

	fName := filepath.Base(f.FileName())

	if strings.HasPrefix(fName, ".") && len(fName) > 1 {
		return true
	}

	return false
}
