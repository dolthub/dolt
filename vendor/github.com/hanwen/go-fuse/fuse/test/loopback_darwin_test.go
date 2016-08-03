package test

import (
	"syscall"
)

func clearStatfs(s *syscall.Statfs_t) {
	empty := syscall.Statfs_t{}
	s.Type = 0
	s.Fsid = empty.Fsid
	// s.Spare = empty.Spare
	// TODO - figure out what this is for.
	s.Flags = 0
}
