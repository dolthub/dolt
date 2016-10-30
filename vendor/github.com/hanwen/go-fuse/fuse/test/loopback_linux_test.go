// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package test

import (
	"io/ioutil"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestTouch(t *testing.T) {
	ts := NewTestCase(t)
	defer ts.Cleanup()

	contents := []byte{1, 2, 3}
	err := ioutil.WriteFile(ts.origFile, []byte(contents), 0700)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	err = os.Chtimes(ts.mountFile, time.Unix(42, 0), time.Unix(43, 0))
	if err != nil {
		t.Fatalf("Chtimes failed: %v", err)
	}

	var stat syscall.Stat_t
	err = syscall.Lstat(ts.mountFile, &stat)
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
	if stat.Atim.Sec != 42 {
		t.Errorf("Got atime.sec %d, want 42. Stat_t was %#v", stat.Atim.Sec, stat)
	}
	if stat.Mtim.Sec != 43 {
		t.Errorf("Got mtime.sec %d, want 43. Stat_t was %#v", stat.Mtim.Sec, stat)
	}
}

func TestNegativeTime(t *testing.T) {
	ts := NewTestCase(t)
	defer ts.Cleanup()

	_, err := os.Create(ts.origFile)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	var stat syscall.Stat_t

	// set negative nanosecond will occur errors on UtimesNano as invalid argument
	ut := time.Date(1960, time.January, 10, 23, 0, 0, 0, time.UTC)
	tim := []syscall.Timespec{
		syscall.NsecToTimespec(ut.UnixNano()),
		syscall.NsecToTimespec(ut.UnixNano()),
	}
	err = syscall.UtimesNano(ts.mountFile, tim)
	if err != nil {
		t.Fatalf("UtimesNano failed: %v", err)
	}
	err = syscall.Lstat(ts.mountFile, &stat)
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}

	if stat.Atim.Sec >= 0 || stat.Mtim.Sec >= 0 {
		t.Errorf("Got wrong timestamps %v", stat)
	}
}

func clearStatfs(s *syscall.Statfs_t) {
	empty := syscall.Statfs_t{}
	s.Type = 0
	s.Fsid = empty.Fsid
	s.Spare = empty.Spare
	// TODO - figure out what this is for.
	s.Flags = 0
}

func TestFallocate(t *testing.T) {
	ts := NewTestCase(t)
	defer ts.Cleanup()
	if ts.state.KernelSettings().Minor < 19 {
		t.Log("FUSE does not support Fallocate.")
		return
	}

	rwFile, err := os.OpenFile(ts.mnt+"/file", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer rwFile.Close()
	err = syscall.Fallocate(int(rwFile.Fd()), 0, 1024, 4096)
	if err != nil {
		t.Fatalf("FUSE Fallocate failed: %v", err)
	}
	fi, err := os.Lstat(ts.orig + "/file")
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
	if fi.Size() < (1024 + 4096) {
		t.Fatalf("fallocate should have changed file size. Got %d bytes",
			fi.Size())
	}
}

// Check that "." and ".." exists. syscall.Getdents is linux specific.
func TestSpecialEntries(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	d, err := os.Open(tc.mnt)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer d.Close()
	buf := make([]byte, 100)
	n, err := syscall.Getdents(int(d.Fd()), buf)
	if n == 0 {
		t.Errorf("directory is empty, entries '.' and '..' are missing")
	}
}
