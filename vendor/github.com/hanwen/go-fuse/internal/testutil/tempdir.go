// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testutil

import (
	"io/ioutil"
	"log"
	"runtime"
	"strings"
)

// TempDir creates a temporary directory that includes the name of the
// testcase. Panics if there was an I/O problem creating the directory.
func TempDir() string {
	frames := make([]uintptr, 10) // at least 1 entry needed
	n := runtime.Callers(1, frames)

	lastName := ""
	for _, pc := range frames[:n] {
		f := runtime.FuncForPC(pc)
		name := f.Name()
		i := strings.LastIndex(name, ".")
		if i >= 0 {
			name = name[i+1:]
		}
		if strings.HasPrefix(name, "Test") {
			lastName = name
		}
	}

	dir, err := ioutil.TempDir("", lastName)
	if err != nil {
		log.Panicf("TempDir(%s): %v", lastName, err)
	}
	return dir
}
