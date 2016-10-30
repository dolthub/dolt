// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package unionfs

import (
	"syscall"
	"unsafe"
)

// Darwin doesn't have support for syscall.Getxattr() so we pull it into its own file and implement it by hand on Darwin.
func Getxattr(path string, attr string, dest []byte) (sz int, err error) {
	var _p0 *byte
	_p0, err = syscall.BytePtrFromString(path)
	if err != nil {
		return
	}
	var _p1 *byte
	_p1, err = syscall.BytePtrFromString(attr)
	if err != nil {
		return
	}
	var _p2 unsafe.Pointer
	if len(dest) > 0 {
		_p2 = unsafe.Pointer(&dest[0])
	} else {
		var _zero uintptr
		_p2 = unsafe.Pointer(&_zero)
	}
	r0, _, e1 := syscall.Syscall6(syscall.SYS_GETXATTR, uintptr(unsafe.Pointer(_p0)), uintptr(unsafe.Pointer(_p1)), uintptr(_p2), uintptr(len(dest)), 0, 0)
	sz = int(r0)
	if e1 != 0 {
		err = e1
	}
	return
}
