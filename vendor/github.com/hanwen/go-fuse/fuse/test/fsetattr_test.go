// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package test

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/go-fuse/internal/testutil"
)

type MutableDataFile struct {
	nodefs.File

	data []byte
	fuse.Attr
	GetAttrCalled bool
	FsyncCalled   bool
}

func (f *MutableDataFile) String() string {
	return "MutableDataFile"
}

func (f *MutableDataFile) Read(buf []byte, off int64) (fuse.ReadResult, fuse.Status) {
	end := int(off) + len(buf)
	if end > len(f.data) {
		end = len(f.data)
	}

	return fuse.ReadResultData(f.data[off:end]), fuse.OK
}

func (f *MutableDataFile) Write(d []byte, off int64) (uint32, fuse.Status) {
	end := int64(len(d)) + off
	if int(end) > len(f.data) {
		data := make([]byte, len(f.data), end)
		copy(data, f.data)
		f.data = data[:end]
	}
	copy(f.data[off:end], d)
	f.Attr.Size = uint64(len(f.data))

	return uint32(end - off), fuse.OK
}

func (f *MutableDataFile) Flush() fuse.Status {
	return fuse.OK
}

func (f *MutableDataFile) Release() {

}

func (f *MutableDataFile) getAttr(out *fuse.Attr) {
	*out = f.Attr
	out.Size = uint64(len(f.data))
}

func (f *MutableDataFile) GetAttr(out *fuse.Attr) fuse.Status {
	f.GetAttrCalled = true
	f.getAttr(out)
	return fuse.OK
}

func (f *MutableDataFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	f.Attr.SetTimes(atime, mtime, nil)
	return fuse.OK
}

func (f *MutableDataFile) Truncate(size uint64) fuse.Status {
	f.data = f.data[:size]
	return fuse.OK
}

func (f *MutableDataFile) Chown(uid uint32, gid uint32) fuse.Status {
	f.Attr.Uid = uid
	f.Attr.Gid = gid
	return fuse.OK
}

func (f *MutableDataFile) Chmod(perms uint32) fuse.Status {
	f.Attr.Mode = (f.Attr.Mode &^ 07777) | perms
	return fuse.OK
}

func (f *MutableDataFile) Fsync(flags int) fuse.Status {
	f.FsyncCalled = true
	return fuse.OK
}

////////////////

// This FS only supports a single r/w file called "/file".
type FSetAttrFs struct {
	pathfs.FileSystem
	file *MutableDataFile
}

func (fs *FSetAttrFs) GetXAttr(name string, attr string, context *fuse.Context) ([]byte, fuse.Status) {
	return nil, fuse.ENOATTR
}

func (fs *FSetAttrFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	if name == "" {
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0700}, fuse.OK
	}
	if name == "file" && fs.file != nil {
		var a fuse.Attr
		fs.file.getAttr(&a)
		a.Mode |= fuse.S_IFREG
		return &a, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (fs *FSetAttrFs) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	if name == "file" {
		return fs.file, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (fs *FSetAttrFs) Create(name string, flags uint32, mode uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	if name == "file" {
		f := NewFile()
		fs.file = f
		fs.file.Attr.Mode = mode
		return f, fuse.OK
	}
	return nil, fuse.ENOENT
}

func NewFile() *MutableDataFile {
	return &MutableDataFile{File: nodefs.NewDefaultFile()}
}

func setupFAttrTest(t *testing.T, fs pathfs.FileSystem) (dir string, clean func()) {
	dir = testutil.TempDir()
	nfs := pathfs.NewPathNodeFs(fs, nil)
	opts := nodefs.NewOptions()
	opts.Debug = testutil.VerboseTest()

	state, _, err := nodefs.MountRoot(dir, nfs.Root(), opts)
	if err != nil {
		t.Fatalf("MountNodeFileSystem failed: %v", err)
	}

	go state.Serve()
	if err := state.WaitMount(); err != nil {
		t.Fatal("WaitMount", err)
	}

	clean = func() {
		if err := state.Unmount(); err != nil {
			t.Errorf("cleanup: Unmount: %v", err)
		} else {
			os.RemoveAll(dir)
		}
	}

	if state.KernelSettings().Flags&fuse.CAP_FILE_OPS == 0 {
		clean()
		t.Skip("Mount does not support file operations")
	}

	return dir, clean
}

func TestFSetAttr(t *testing.T) {
	fSetAttrFs := &FSetAttrFs{
		FileSystem: pathfs.NewDefaultFileSystem(),
	}
	fs := pathfs.NewLockingFileSystem(fSetAttrFs)
	dir, clean := setupFAttrTest(t, fs)
	defer func() {
		if clean != nil {
			clean()
		}
	}()

	fn := dir + "/file"
	f, err := os.OpenFile(fn, os.O_CREATE|os.O_WRONLY, 0755)

	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	_, err = f.WriteString("hello")
	if err != nil {
		t.Fatalf("WriteString failed: %v", err)
	}

	code := syscall.Ftruncate(int(f.Fd()), 3)
	if code != nil {
		t.Error("truncate retval", os.NewSyscallError("Ftruncate", code))
	}

	if a, status := fs.GetAttr("file", nil); !status.Ok() {
		t.Fatalf("GetAttr: status %v", status)
	} else if a.Size != 3 {
		t.Errorf("truncate: size %d, status %v", a.Size, status)
	}

	if err := f.Chmod(024); err != nil {
		t.Fatalf("Chmod failed: %v", err)
	}

	if a, status := fs.GetAttr("file", nil); !status.Ok() {
		t.Errorf("chmod: %v", status)
	} else if a.Mode&07777 != 024 {
		t.Errorf("getattr after chmod: %o", a.Mode&0777)
	}

	if err := os.Chtimes(fn, time.Unix(0, 100e3), time.Unix(0, 101e3)); err != nil {
		t.Fatalf("Chtimes failed: %v", err)
	}

	if a, status := fs.GetAttr("file", nil); !status.Ok() {
		t.Errorf("GetAttr: %v", status)
	} else if a.Atimensec != 100e3 || a.Mtimensec != 101e3 {
		t.Errorf("Utimens: atime %d != 100e3 mtime %d != 101e3",
			a.Atimensec, a.Mtimensec)
	}

	newFi, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	i1 := fuse.ToStatT(fi).Ino
	i2 := fuse.ToStatT(newFi).Ino
	if i1 != i2 {
		t.Errorf("f.Lstat().Ino = %d. Returned %d before.", i2, i1)
	}

	if code := syscall.Fsync(int(f.Fd())); code != nil {
		t.Error("Fsync failed:", os.NewSyscallError("Fsync", code))
	}

	// Close the file, otherwise we can't unmount.
	f.Close()

	// Shutdown the FUSE FS so we can safely look at fSetAttrFs
	clean()
	clean = nil
	if !fSetAttrFs.file.FsyncCalled {
		t.Error("Fsync was not called")
	}
}
