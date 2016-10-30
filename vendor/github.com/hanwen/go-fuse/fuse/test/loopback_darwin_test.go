// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package test

import (
	"syscall"
)

func clearStatfs(s *syscall.Statfs_t) {
	empty := syscall.Statfs_t{}

	// FUSE can only set the following fields.
	empty.Blocks = s.Blocks
	empty.Bfree = s.Bfree
	empty.Bavail = s.Bavail
	empty.Files = s.Files
	empty.Ffree = s.Ffree
	empty.Iosize = s.Iosize
	empty.Bsize = s.Bsize
	// Clear out the rest.
	*s = empty
}
