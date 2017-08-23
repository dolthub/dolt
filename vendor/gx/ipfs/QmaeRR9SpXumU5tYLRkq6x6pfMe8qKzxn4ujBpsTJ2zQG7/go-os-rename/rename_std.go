// +build !windows

package osrename

import "os"

func Rename(src, dst string) error {
	return os.Rename(src, dst)
}
