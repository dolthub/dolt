// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package unionfs

import (
	"io/ioutil"
	"testing"
)

func TestUnionFsFdLeak(t *testing.T) {
	beforeEntries, err := ioutil.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	wd, clean := setupUfs(t)
	err = ioutil.WriteFile(wd+"/ro/file", []byte("blablabla"), 0644)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	setRecursiveWritable(t, wd+"/ro", false)

	contents, err := ioutil.ReadFile(wd + "/mnt/file")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	err = ioutil.WriteFile(wd+"/mnt/file", contents, 0644)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	clean()

	afterEntries, err := ioutil.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	if len(afterEntries) != len(beforeEntries) {
		t.Errorf("/proc/self/fd changed size: after %v before %v", len(beforeEntries), len(afterEntries))
	}
}
