// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package file

import (
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/attic-labs/noms/go/d"
)

// DumbCopy copies the contents of a regular file at srcPath (following symlinks) to a new regular file at dstPath. New file is created with same mode.
func DumbCopy(srcPath, dstPath string) {
	chkClose := func(c io.Closer) { d.PanicIfError(c.Close()) }

	info, err := os.Stat(srcPath)
	d.PanicIfError(err)

	src, err := os.Open(srcPath)
	d.PanicIfError(err)
	defer chkClose(src)

	dst, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, info.Mode())
	d.PanicIfError(err)
	defer chkClose(dst)
	_, err = io.Copy(dst, src)
	d.PanicIfError(err)
}

// MyDir returns the directory in which the file containing the calling source code resides.
func MyDir() string {
	_, path, _, ok := runtime.Caller(1)
	if !ok {
		d.Panic("Should have been able to get Caller.")
	}
	return filepath.Dir(path)
}
