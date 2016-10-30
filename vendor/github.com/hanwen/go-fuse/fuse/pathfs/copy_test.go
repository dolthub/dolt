// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pathfs

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/hanwen/go-fuse/internal/testutil"
)

func TestCopyFile(t *testing.T) {
	d1 := testutil.TempDir()
	defer os.RemoveAll(d1)
	d2 := testutil.TempDir()
	defer os.RemoveAll(d2)

	fs1 := NewLoopbackFileSystem(d1)
	fs2 := NewLoopbackFileSystem(d2)

	content1 := "blabla"

	err := ioutil.WriteFile(d1+"/file", []byte(content1), 0644)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	code := CopyFile(fs1, fs2, "file", "file", nil)
	if !code.Ok() {
		t.Fatal("Unexpected ret code", code)
	}

	data, err := ioutil.ReadFile(d2 + "/file")
	if content1 != string(data) {
		t.Fatal("Unexpected content", string(data))
	}

	content2 := "foobar"

	err = ioutil.WriteFile(d2+"/file", []byte(content2), 0644)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Copy back: should overwrite.
	code = CopyFile(fs2, fs1, "file", "file", nil)
	if !code.Ok() {
		t.Fatal("Unexpected ret code", code)
	}

	data, err = ioutil.ReadFile(d1 + "/file")
	if content2 != string(data) {
		t.Fatal("Unexpected content", string(data))
	}

}
