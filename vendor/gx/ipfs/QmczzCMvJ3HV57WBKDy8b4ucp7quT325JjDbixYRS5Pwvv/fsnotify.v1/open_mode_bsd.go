// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build freebsd openbsd netbsd dragonfly

package fsnotify

import "gx/ipfs/QmTsTrxKNXu8sZLv7FP6p884CHoDbT9upKA1j4FkCy5V7T/sys/unix"

const openMode = unix.O_NONBLOCK | unix.O_RDONLY
