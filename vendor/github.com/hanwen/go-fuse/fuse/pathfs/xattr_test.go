// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build linux

package pathfs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/internal/testutil"
)

var xattrGolden = map[string][]byte{
	"user.attr1": []byte("val1"),
	"user.attr2": []byte("val2")}
var xattrFilename = "filename"

type XAttrTestFs struct {
	filename string
	attrs    map[string][]byte

	FileSystem
}

func NewXAttrFs(nm string, m map[string][]byte) *XAttrTestFs {
	x := &XAttrTestFs{
		filename:   nm,
		attrs:      make(map[string][]byte, len(m)),
		FileSystem: NewDefaultFileSystem(),
	}

	for k, v := range m {
		x.attrs[k] = v
	}
	return x
}

func (fs *XAttrTestFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	a := &fuse.Attr{}
	if name == "" || name == "/" {
		a.Mode = fuse.S_IFDIR | 0700
		return a, fuse.OK
	}
	if name == fs.filename {
		a.Mode = fuse.S_IFREG | 0600
		return a, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (fs *XAttrTestFs) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	if name != fs.filename {
		return fuse.ENOENT
	}
	dest := make([]byte, len(data))
	copy(dest, data)
	fs.attrs[attr] = dest
	return fuse.OK
}

func (fs *XAttrTestFs) GetXAttr(name string, attr string, context *fuse.Context) ([]byte, fuse.Status) {
	if name != fs.filename {
		return nil, fuse.ENOENT
	}
	v, ok := fs.attrs[attr]
	if !ok {
		return nil, fuse.ENOATTR
	}
	return v, fuse.OK
}

func (fs *XAttrTestFs) ListXAttr(name string, context *fuse.Context) (data []string, code fuse.Status) {
	if name != fs.filename {
		return nil, fuse.ENOENT
	}

	for k := range fs.attrs {
		data = append(data, k)
	}
	return data, fuse.OK
}

func (fs *XAttrTestFs) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
	if name != fs.filename {
		return fuse.ENOENT
	}
	_, ok := fs.attrs[attr]
	if !ok {
		return fuse.ENOATTR
	}
	delete(fs.attrs, attr)
	return fuse.OK
}

func readXAttr(p, a string) (val []byte, err error) {
	val = make([]byte, 1024)
	return getXAttr(p, a, val)
}

func xattrTestCase(t *testing.T, nm string, m map[string][]byte) (mountPoint string, cleanup func()) {
	xfs := NewXAttrFs(nm, m)
	mountPoint = testutil.TempDir()

	nfs := NewPathNodeFs(xfs, nil)
	state, _, err := nodefs.MountRoot(mountPoint, nfs.Root(),
		&nodefs.Options{Debug: VerboseTest()})
	if err != nil {
		t.Fatalf("MountRoot failed: %v", err)
	}

	go state.Serve()
	return mountPoint, func() {
		state.Unmount()
		os.RemoveAll(mountPoint)
	}
}

func TestXAttrNoAttrs(t *testing.T) {
	nm := xattrFilename
	mountPoint, clean := xattrTestCase(t, nm, make(map[string][]byte))
	defer clean()

	mounted := filepath.Join(mountPoint, nm)
	attrs, err := listXAttr(mounted)
	if err != nil {
		t.Error("Unexpected ListXAttr error", err)
	}

	if len(attrs) > 0 {
		t.Errorf("ListXAttr(%s) = %s, want empty slice", mounted, attrs)
	}
}

func TestXAttrNoExist(t *testing.T) {
	nm := xattrFilename
	mountPoint, clean := xattrTestCase(t, nm, xattrGolden)
	defer clean()

	mounted := filepath.Join(mountPoint, nm)
	_, err := os.Lstat(mounted)
	if err != nil {
		t.Error("Unexpected stat error", err)
	}

	val, err := readXAttr(mounted, "noexist")
	if err == nil {
		t.Error("Expected GetXAttr error", val)
	}
}

func TestXAttrRead(t *testing.T) {
	nm := xattrFilename
	mountPoint, clean := xattrTestCase(t, nm, xattrGolden)
	defer clean()

	mounted := filepath.Join(mountPoint, nm)
	attrs, err := listXAttr(mounted)
	readback := make(map[string][]byte)
	if err != nil {
		t.Error("Unexpected ListXAttr error", err)
	} else {
		for _, a := range attrs {
			val, err := readXAttr(mounted, a)
			if err != nil {
				t.Errorf("GetXAttr(%q) failed: %v", a, err)
			}
			readback[a] = val
		}
	}

	if len(readback) != len(xattrGolden) {
		t.Error("length mismatch", xattrGolden, readback)
	} else {
		for k, v := range readback {
			if bytes.Compare(xattrGolden[k], v) != 0 {
				t.Error("val mismatch", k, v, xattrGolden[k])
			}
		}
	}

	err = sysSetxattr(mounted, "third", []byte("value"), 0)
	if err != nil {
		t.Error("Setxattr error", err)
	}
	val, err := readXAttr(mounted, "third")
	if err != nil || string(val) != "value" {
		t.Error("Read back set xattr:", err, string(val))
	}

	sysRemovexattr(mounted, "third")
	val, err = readXAttr(mounted, "third")
	if fuse.ToStatus(err) != fuse.ENOATTR {
		t.Error("Data not removed?", err, val)
	}
}
