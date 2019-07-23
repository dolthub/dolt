// +build !windows

package osutil

import (
	"path/filepath"
	"strings"
)

const (
	IsWindows     = false
	PathDelimiter = string(byte(filepath.Separator))
)

var (
	SystemVolume   = ""
	FileSystemRoot = PathDelimiter
)

// PathToNative will convert a Unix path into the Windows-native variant (only if running on a Windows machine)
func PathToNative(p string) string {
	if len(p) == 0 {
		return p
	}
	p = strings.ReplaceAll(p, `\`, PathDelimiter)
	if StartsWithWindowsVolume(p) {
		p = p[2:]
	}
	return p
}

// IsWindowsSharingViolation returns if the error is a Windows sharing violation
func IsWindowsSharingViolation(err error) bool {
	return false
}
