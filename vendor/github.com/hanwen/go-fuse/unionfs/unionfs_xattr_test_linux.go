// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package unionfs

import (
	"syscall"
)

// Darwin doesn't have support for syscall.Getxattr() so we pull it into its own file and implement it by hand on Darwin.
func Getxattr(path string, attr string, dest []byte) (sz int, err error) {
	return syscall.Getxattr(path, attr, dest)
}
