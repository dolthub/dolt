// Copyright 2009 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build darwin dragonfly freebsd linux netbsd openbsd solaris

package reuseport

import (
	"os"
	"syscall"
)

func boolint(b bool) int {
	if b {
		return 1
	}
	return 0
}

func setNoDelay(fd int, noDelay bool) error {
	return os.NewSyscallError("setsockopt", syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, boolint(noDelay)))
}

func setLinger(fd int, sec int) error {
	var l syscall.Linger
	if sec >= 0 {
		l.Onoff = 1
		l.Linger = int32(sec)
	} else {
		l.Onoff = 0
		l.Linger = 0
	}
	return os.NewSyscallError("setsockopt", syscall.SetsockoptLinger(fd, syscall.SOL_SOCKET, syscall.SO_LINGER, &l))
}
