// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build linux

package test

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"unsafe"
)

func clen(n []byte) int {
	for i := 0; i < len(n); i++ {
		if n[i] == 0 {
			return i
		}
	}
	return len(n)
}

type CDirent struct {
	ino   uint64
	ntype uint8
	off   int64
	name  string
}

func parseDirents(buf []byte) []CDirent {
	var result []CDirent
	for len(buf) > 0 {
		de := *(*syscall.Dirent)(unsafe.Pointer(&buf[0]))
		buf = buf[de.Reclen:]
		bytes := (*[10000]byte)(unsafe.Pointer(&de.Name[0]))
		var name = string(bytes[0:clen(bytes[:])])
		result = append(result, CDirent{
			ino:   de.Ino,
			ntype: de.Type,
			off:   de.Off,
			name:  name,
		})
	}
	return result
}

// This test is inspired on xfstest, t_dir_offset2.c
func TestReaddirPlusSeek(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	var names []string
	for i := 0; i < 20; i++ {
		names = append(names, fmt.Sprintf("abcd%d", i))
	}

	for _, n := range names {
		if err := os.MkdirAll(filepath.Join(tc.origSubdir, n), 0755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}
	}

	fd, err := syscall.Open(tc.mountSubdir, syscall.O_RDONLY, 0755)
	if err != nil {
		t.Fatalf("Open(%q): %v", tc.mountSubdir, err)
	}
	defer syscall.Close(int(fd))

	// store offsets
	type entryOff struct {
		ino uint64
		off int64
	}
	previous := map[int]entryOff{}
	var bufdata [1024]byte
	for {
		buf := bufdata[:]
		n, err := syscall.ReadDirent(fd, buf)
		if err != nil {
			t.Fatalf("ReadDirent: %v", err)
		}
		if n == 0 {
			break
		}

		buf = buf[:n]
		for _, d := range parseDirents(buf) {
			if d.name == "." || d.name == ".." {
				continue
			}
			i := len(previous)
			previous[i] = entryOff{d.ino, d.off}
		}
	}

	for i := len(previous) - 1; i >= 0; i-- {
		var off int64
		if i > 0 {
			off = previous[i-1].off
		}

		if _, err := syscall.Seek(fd, off, 0); err != nil {
			t.Fatalf("Seek %v", err)
		}

		buf := bufdata[:]
		n, err := syscall.ReadDirent(fd, buf)
		if err != nil {
			t.Fatalf("readdir after seek %d: %v", i, err)
		}
		if n == 0 {
			t.Fatalf("no dirent after seek to %d", i)
		}

		ds := parseDirents(buf[:n])
		if ds[0].ino != previous[i].ino {
			t.Errorf("got ino %d, want %d", ds[0].ino, previous[i].ino)
		}
	}

	// Delete has a forget as side-effect: make sure we get the lookup counts correct.
	for _, n := range names {
		full := filepath.Join(tc.mountSubdir, n)
		if err := syscall.Rmdir(full); err != nil {
			t.Fatalf("Rmdir(%q): %v", n, err)
		}
	}
}
