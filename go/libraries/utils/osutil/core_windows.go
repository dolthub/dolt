// +build windows

package osutil

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

const (
	IsWindows     = true
	PathDelimiter = string(byte(filepath.Separator))
)

var (
	SystemVolume   = filepath.VolumeName(os.Getenv("SYSTEMROOT"))
	FileSystemRoot = SystemVolume + PathDelimiter
)

// PathToNative will convert a Unix path into the Windows-native variant (only if running on a Windows machine)
func PathToNative(p string) string {
	if len(p) == 0 {
		return p
	}
	p = filepath.FromSlash(p)
	if !StartsWithWindowsVolume(p) {
		if p[0] == PathDelimiter[0] {
			p = SystemVolume + p
		} else {
			p = FileSystemRoot + p
		}
	}
	return p
}

// IsWindowsSharingViolation returns if the error is a Windows sharing violation
func IsWindowsSharingViolation(err error) bool {
	if pathErr, ok := err.(*os.PathError); ok && pathErr != nil && pathErr.Err == windows.ERROR_SHARING_VIOLATION {
		return true
	}
	return false
}
