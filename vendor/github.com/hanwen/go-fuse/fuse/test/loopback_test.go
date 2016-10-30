// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/go-fuse/internal/testutil"
)

const mode uint32 = 0757

type testCase struct {
	tmpDir string
	orig   string
	mnt    string

	mountFile   string
	mountSubdir string
	origFile    string
	origSubdir  string
	tester      *testing.T
	state       *fuse.Server
	pathFs      *pathfs.PathNodeFs
	connector   *nodefs.FileSystemConnector
}

const testTtl = 100 * time.Millisecond

// Mkdir is a utility wrapper for os.Mkdir, aborting the test if it fails.
func (tc *testCase) Mkdir(name string, mode os.FileMode) {
	if err := os.Mkdir(name, mode); err != nil {
		tc.tester.Fatalf("Mkdir(%q,%v): %v", name, mode, err)
	}
}

// WriteFile is a utility wrapper for ioutil.WriteFile, aborting the
// test if it fails.
func (tc *testCase) WriteFile(name string, content []byte, mode os.FileMode) {
	if err := ioutil.WriteFile(name, content, mode); err != nil {
		if len(content) > 50 {
			content = append(content[:50], '.', '.', '.')
		}

		tc.tester.Fatalf("WriteFile(%q, %q, %o): %v", name, content, mode, err)
	}
}

// Create and mount filesystem.
func NewTestCase(t *testing.T) *testCase {
	tc := &testCase{}
	tc.tester = t

	// Make sure system setting does not affect test.
	syscall.Umask(0)

	const name string = "hello.txt"
	const subdir string = "subdir"

	var err error
	tc.tmpDir = testutil.TempDir()
	tc.orig = tc.tmpDir + "/orig"
	tc.mnt = tc.tmpDir + "/mnt"

	tc.Mkdir(tc.orig, 0700)
	tc.Mkdir(tc.mnt, 0700)

	tc.mountFile = filepath.Join(tc.mnt, name)
	tc.mountSubdir = filepath.Join(tc.mnt, subdir)
	tc.origFile = filepath.Join(tc.orig, name)
	tc.origSubdir = filepath.Join(tc.orig, subdir)

	var pfs pathfs.FileSystem
	pfs = pathfs.NewLoopbackFileSystem(tc.orig)
	pfs = pathfs.NewLockingFileSystem(pfs)

	tc.pathFs = pathfs.NewPathNodeFs(pfs, &pathfs.PathNodeFsOptions{
		ClientInodes: true})
	tc.connector = nodefs.NewFileSystemConnector(tc.pathFs.Root(),
		&nodefs.Options{
			EntryTimeout:    testTtl,
			AttrTimeout:     testTtl,
			NegativeTimeout: 0.0,
			Debug:           testutil.VerboseTest(),
		})
	tc.state, err = fuse.NewServer(
		fuse.NewRawFileSystem(tc.connector.RawFS()), tc.mnt, &fuse.MountOptions{
			SingleThreaded: true,
			Debug:          testutil.VerboseTest(),
		})
	if err != nil {
		t.Fatal("NewServer:", err)
	}

	go tc.state.Serve()
	if err := tc.state.WaitMount(); err != nil {
		t.Fatal("WaitMount", err)
	}
	return tc
}

// Unmount and del.
func (tc *testCase) Cleanup() {
	err := tc.state.Unmount()
	if err != nil {
		tc.tester.Fatalf("Unmount failed: %v", err)
	}
	os.RemoveAll(tc.tmpDir)
}

func (tc *testCase) rootNode() *nodefs.Inode {
	return tc.pathFs.Root().Inode()
}

////////////////
// Tests.

func TestOpenUnreadable(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()
	_, err := os.Open(tc.mnt + "/doesnotexist")
	if err == nil {
		t.Errorf("open non-existent should raise error")
	}
}

func TestReadThrough(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	content := randomData(125)
	tc.WriteFile(tc.origFile, content, 0700)
	err := os.Chmod(tc.mountFile, os.FileMode(mode))
	if err != nil {
		t.Fatalf("Chmod failed: %v", err)
	}

	fi, err := os.Lstat(tc.mountFile)
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
	if uint32(fi.Mode().Perm()) != mode {
		t.Errorf("Wrong mode %o != %o", int(fi.Mode().Perm()), mode)
	}

	// Open (for read), read.
	f, err := os.Open(tc.mountFile)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer f.Close()

	var buf [1024]byte
	slice := buf[:]
	n, err := f.Read(slice)
	CompareSlices(t, slice[:n], content)
}

func TestRemove(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	contents := []byte{1, 2, 3}
	tc.WriteFile(tc.origFile, []byte(contents), 0700)

	err := os.Remove(tc.mountFile)
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	_, err = os.Lstat(tc.origFile)
	if err == nil {
		t.Errorf("Lstat() after delete should have generated error.")
	}
}

func TestWriteThrough(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	// Create (for write), write.
	f, err := os.OpenFile(tc.mountFile, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer f.Close()

	content := randomData(125)
	n, err := f.Write(content)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(content) {
		t.Errorf("Write mismatch: %v of %v", n, len(content))
	}

	fi, err := os.Lstat(tc.origFile)
	if err != nil {
		t.Fatalf("Lstat(%q): %v", tc.origFile, err)
	}
	if fi.Mode().Perm() != 0644 {
		t.Errorf("create mode error %o", fi.Mode()&0777)
	}

	f, err = os.Open(tc.origFile)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer f.Close()

	var buf [1024]byte
	slice := buf[:]
	n, err = f.Read(slice)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	CompareSlices(t, slice[:n], content)
}

func TestMkdirRmdir(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	// Mkdir/Rmdir.
	if err := os.Mkdir(tc.mountSubdir, 0777); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	if fi, err := os.Lstat(tc.origSubdir); err != nil {
		t.Fatalf("Lstat(%q): %v", tc.origSubdir, err)
	} else if !fi.IsDir() {
		t.Errorf("Not a directory: %v", fi)
	}

	if err := os.Remove(tc.mountSubdir); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
}

func TestLinkCreate(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	content := randomData(125)
	tc.WriteFile(tc.origFile, content, 0700)

	tc.Mkdir(tc.origSubdir, 0777)

	// Link.
	mountSubfile := filepath.Join(tc.mountSubdir, "subfile")
	err := os.Link(tc.mountFile, mountSubfile)
	if err != nil {
		t.Fatalf("Link failed: %v", err)
	}

	var subStat, stat syscall.Stat_t
	err = syscall.Lstat(mountSubfile, &subStat)
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
	err = syscall.Lstat(tc.mountFile, &stat)
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}

	if stat.Nlink != 2 {
		t.Errorf("Expect 2 links: %v", stat)
	}
	if stat.Ino != subStat.Ino {
		t.Errorf("Link succeeded, but inode numbers different: %v %v", stat.Ino, subStat.Ino)
	}
	readback, err := ioutil.ReadFile(mountSubfile)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	CompareSlices(t, readback, content)

	err = os.Remove(tc.mountFile)
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	_, err = ioutil.ReadFile(mountSubfile)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
}

func randomData(size int) []byte {
	return bytes.Repeat([]byte{'x'}, size)
}

// Deal correctly with hard links implied by matching client inode
// numbers.
func TestLinkExisting(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	c := randomData(5)

	tc.WriteFile(tc.orig+"/file1", c, 0644)

	err := os.Link(tc.orig+"/file1", tc.orig+"/file2")
	if err != nil {
		t.Fatalf("Link failed: %v", err)
	}

	var s1, s2 syscall.Stat_t
	err = syscall.Lstat(tc.mnt+"/file1", &s1)
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
	err = syscall.Lstat(tc.mnt+"/file2", &s2)
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}

	if s1.Ino != s2.Ino {
		t.Errorf("linked files should have identical inodes %v %v", s1.Ino, s2.Ino)
	}

	back, err := ioutil.ReadFile(tc.mnt + "/file1")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	CompareSlices(t, back, c)
}

// Deal correctly with hard links implied by matching client inode
// numbers.
func TestLinkForget(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	c := "hello"

	tc.WriteFile(tc.orig+"/file1", []byte(c), 0644)
	err := os.Link(tc.orig+"/file1", tc.orig+"/file2")
	if err != nil {
		t.Fatalf("Link failed: %v", err)
	}

	for _, fn := range []string{"file1", "file2"} {
		var s syscall.Stat_t
		err = syscall.Lstat(tc.mnt+"/"+fn, &s)
		if err != nil {
			t.Fatalf("Lstat failed: %v", err)
		}
		tc.pathFs.ForgetClientInodes()
	}

	// Now, the backing files are still hardlinked, but go-fuse's
	// view of them should not be because of the
	// ForgetClientInodes call. To prove this, we swap out the
	// files in the backing store, and prove that they are
	// distinct by truncating to different lengths.
	for _, fn := range []string{"file1", "file2"} {
		fn = tc.orig + "/" + fn
		if err := os.Remove(fn); err != nil {
			t.Fatalf("Remove", err)
		}
		tc.WriteFile(fn, []byte(c), 0644)
	}
	for i, fn := range []string{"file1", "file2"} {
		fn = tc.mnt + "/" + fn
		if err := os.Truncate(fn, int64(i)); err != nil {
			t.Fatalf("Truncate", err)
		}
	}

	for i, fn := range []string{"file1", "file2"} {
		var s syscall.Stat_t
		err = syscall.Lstat(tc.mnt+"/"+fn, &s)
		if err != nil {
			t.Fatalf("Lstat failed: %v", err)
		}
		if s.Size != int64(i) {
			t.Errorf("Lstat(%q): got size %d, want %d", fn, s.Size, i)
		}
	}
}

func TestSymlink(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	contents := []byte{1, 2, 3}
	tc.WriteFile(tc.origFile, []byte(contents), 0700)

	linkFile := "symlink-file"
	orig := "hello.txt"
	err := os.Symlink(orig, filepath.Join(tc.mnt, linkFile))

	if err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	origLink := filepath.Join(tc.orig, linkFile)
	fi, err := os.Lstat(origLink)
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}

	if fi.Mode()&os.ModeSymlink == 0 {
		t.Errorf("not a symlink: %v", fi)
		return
	}

	read, err := os.Readlink(filepath.Join(tc.mnt, linkFile))
	if err != nil {
		t.Fatalf("Readlink failed: %v", err)
	}

	if read != orig {
		t.Errorf("unexpected symlink value '%v'", read)
	}
}

func TestRename(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	contents := []byte{1, 2, 3}
	tc.WriteFile(tc.origFile, []byte(contents), 0700)
	sd := tc.mnt + "/testRename"
	tc.Mkdir(sd, 0777)

	subFile := sd + "/subfile"
	if err := os.Rename(tc.mountFile, subFile); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	f, _ := os.Lstat(tc.origFile)
	if f != nil {
		t.Errorf("original %v still exists.", tc.origFile)
	}
	if _, err := os.Lstat(subFile); err != nil {
		t.Errorf("destination %q does not exist: %v", subFile, err)
	}
}

// Flaky test, due to rename race condition.
func TestDelRename(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	sd := tc.mnt + "/testDelRename"
	tc.Mkdir(sd, 0755)

	d := sd + "/dest"
	tc.WriteFile(d, []byte("blabla"), 0644)

	f, err := os.Open(d)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer f.Close()

	if err := os.Remove(d); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	s := sd + "/src"
	tc.WriteFile(s, []byte("blabla"), 0644)
	if err := os.Rename(s, d); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
}

func TestOverwriteRename(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	sd := tc.mnt + "/testOverwriteRename"
	tc.Mkdir(sd, 0755)

	d := sd + "/dest"
	tc.WriteFile(d, []byte("blabla"), 0644)

	s := sd + "/src"
	tc.WriteFile(s, []byte("blabla"), 0644)

	if err := os.Rename(s, d); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
}

func TestAccess(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Log("Skipping TestAccess() as root.")
		return
	}
	tc := NewTestCase(t)
	defer tc.Cleanup()

	contents := []byte{1, 2, 3}
	tc.WriteFile(tc.origFile, []byte(contents), 0700)
	if err := os.Chmod(tc.origFile, 0); err != nil {
		t.Fatalf("Chmod failed: %v", err)
	}
	// Ugh - copied from unistd.h
	const W_OK uint32 = 2

	if errCode := syscall.Access(tc.mountFile, W_OK); errCode != syscall.EACCES {
		t.Errorf("Expected EACCES for non-writable, %v %v", errCode, syscall.EACCES)
	}

	if err := os.Chmod(tc.origFile, 0222); err != nil {
		t.Fatalf("Chmod failed: %v", err)
	}

	if errCode := syscall.Access(tc.mountFile, W_OK); errCode != nil {
		t.Errorf("Expected no error code for writable. %v", errCode)
	}
}

func TestMknod(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	if errNo := syscall.Mknod(tc.mountFile, syscall.S_IFIFO|0777, 0); errNo != nil {
		t.Errorf("Mknod %v", errNo)
	}

	if fi, err := os.Lstat(tc.origFile); err != nil {
		t.Errorf("Lstat(%q): %v", tc.origFile, err)
	} else if fi.Mode()&os.ModeNamedPipe == 0 {
		t.Errorf("Expected FIFO filetype, got %x", fi.Mode())
	}
}

func TestReaddir(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	contents := []byte{1, 2, 3}
	tc.WriteFile(tc.origFile, []byte(contents), 0700)
	tc.Mkdir(tc.origSubdir, 0777)

	dir, err := os.Open(tc.mnt)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer dir.Close()

	infos, err := dir.Readdir(10)
	if err != nil {
		t.Fatalf("Readdir failed: %v", err)
	}

	wanted := map[string]bool{
		"hello.txt": true,
		"subdir":    true,
	}
	if len(wanted) != len(infos) {
		t.Errorf("Length mismatch %v", infos)
	} else {
		for _, v := range infos {
			_, ok := wanted[v.Name()]
			if !ok {
				t.Errorf("Unexpected name %v", v.Name())
			}
		}
	}
}

func TestFSync(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	contents := []byte{1, 2, 3}
	tc.WriteFile(tc.origFile, []byte(contents), 0700)

	f, err := os.OpenFile(tc.mountFile, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile(%q): %v", tc.mountFile, err)
	}
	defer f.Close()

	if _, err := f.WriteString("hello there"); err != nil {
		t.Fatalf("WriteString failed: %v", err)
	}

	// How to really test fsync ?
	err = syscall.Fsync(int(f.Fd()))
	if err != nil {
		t.Errorf("fsync returned: %v", err)
	}
}

func TestReadZero(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()
	tc.WriteFile(tc.origFile, []byte{}, 0644)

	back, err := ioutil.ReadFile(tc.mountFile)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", tc.mountFile, err)
	} else if len(back) != 0 {
		t.Errorf("content length: got %d want %d", len(back), 0)
	}
}

func CompareSlices(t *testing.T, got, want []byte) {
	if len(got) != len(want) {
		t.Errorf("content length: got %d want %d", len(got), len(want))
		return
	}

	for i := range want {
		if want[i] != got[i] {
			t.Errorf("content mismatch byte %d, got %d want %d.", i, got[i], want[i])
			break
		}
	}
}

// Check that reading large files doesn't lead to large allocations.
func TestReadLargeMemCheck(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	content := randomData(385 * 1023)
	tc.WriteFile(tc.origFile, []byte(content), 0644)

	f, err := os.Open(tc.mountFile)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer f.Close()

	buf := make([]byte, len(content)+1024)
	f.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	f.Close()
	runtime.GC()
	var before, after runtime.MemStats

	N := 100
	runtime.ReadMemStats(&before)
	for i := 0; i < N; i++ {
		f, _ := os.Open(tc.mountFile)
		f.Read(buf)
		f.Close()
	}
	runtime.ReadMemStats(&after)
	delta := int((after.TotalAlloc - before.TotalAlloc))
	delta = (delta - 40000) / N
	t.Logf("bytes per read loop: %d", delta)
}

func TestReadLarge(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	content := randomData(385 * 1023)
	tc.WriteFile(tc.origFile, []byte(content), 0644)

	back, err := ioutil.ReadFile(tc.mountFile)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	CompareSlices(t, back, content)
}

func TestWriteLarge(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	content := randomData(385 * 1023)
	tc.WriteFile(tc.mountFile, []byte(content), 0644)

	back, err := ioutil.ReadFile(tc.origFile)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	CompareSlices(t, back, content)
}

func randomLengthString(length int) string {
	r := rand.Intn(length)

	b := make([]byte, r)
	for i := 0; i < r; i++ {
		b[i] = byte(i%10) + byte('0')
	}
	return string(b)
}

func TestLargeDirRead(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	created := 100

	names := make([]string, created)

	subdir := filepath.Join(tc.orig, "readdirSubdir")

	tc.Mkdir(subdir, 0700)

	longname := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

	nameSet := make(map[string]bool)
	for i := 0; i < created; i++ {
		// Should vary file name length.
		base := fmt.Sprintf("file%d%s", i,
			randomLengthString(len(longname)))
		name := filepath.Join(subdir, base)

		nameSet[base] = true

		tc.WriteFile(name, []byte("bla"), 0777)

		names[i] = name
	}

	dir, err := os.Open(filepath.Join(tc.mnt, "readdirSubdir"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer dir.Close()

	// Chunked read.
	total := 0
	readSet := make(map[string]bool)
	for {
		namesRead, err := dir.Readdirnames(200)
		if len(namesRead) == 0 || err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Readdirnames failed: %v", err)
		}
		for _, v := range namesRead {
			readSet[v] = true
		}
		total += len(namesRead)
	}

	if total != created {
		t.Errorf("readdir mismatch got %v wanted %v", total, created)
	}
	for k := range nameSet {
		_, ok := readSet[k]
		if !ok {
			t.Errorf("Name %v not found in output", k)
		}
	}
}

func TestRootDir(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	d, err := os.Open(tc.mnt)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if _, err := d.Readdirnames(-1); err != nil {
		t.Fatalf("Readdirnames failed: %v", err)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func ioctl(fd int, cmd int, arg uintptr) (int, int) {
	r0, _, e1 := syscall.Syscall(
		syscall.SYS_IOCTL, uintptr(fd), uintptr(cmd), uintptr(arg))
	val := int(r0)
	errno := int(e1)
	return val, errno
}

func TestIoctl(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	f, err := os.OpenFile(filepath.Join(tc.mnt, "hello.txt"),
		os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer f.Close()
	ioctl(int(f.Fd()), 0x5401, 42)
}

// This test is racy. If an external process consumes space while this
// runs, we may see spurious differences between the two statfs() calls.
func TestStatFs(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	empty := syscall.Statfs_t{}
	s1 := empty
	if err := syscall.Statfs(tc.orig, &s1); err != nil {
		t.Fatal("statfs orig", err)
	}

	s2 := syscall.Statfs_t{}
	if err := syscall.Statfs(tc.mnt, &s2); err != nil {
		t.Fatal("statfs mnt", err)
	}

	clearStatfs(&s1)
	clearStatfs(&s2)
	if !reflect.DeepEqual(s1, s2) {
		t.Errorf("statfs mismatch %#v != %#v", s1, s2)
	}
}

func TestFStatFs(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	fOrig, err := os.OpenFile(tc.orig+"/file", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer fOrig.Close()

	empty := syscall.Statfs_t{}
	s1 := empty
	if errno := syscall.Fstatfs(int(fOrig.Fd()), &s1); errno != nil {
		t.Fatal("statfs orig", err)
	}

	fMnt, err := os.OpenFile(tc.mnt+"/file", os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer fMnt.Close()
	s2 := empty

	if errno := syscall.Fstatfs(int(fMnt.Fd()), &s2); errno != nil {
		t.Fatal("statfs mnt", err)
	}

	clearStatfs(&s1)
	clearStatfs(&s2)
	if !reflect.DeepEqual(s1, s2) {
		t.Errorf("statfs mismatch: %#v != %#v", s1, s2)
	}
}

func TestOriginalIsSymlink(t *testing.T) {
	tmpDir := testutil.TempDir()
	defer os.RemoveAll(tmpDir)
	orig := tmpDir + "/orig"
	err := os.Mkdir(orig, 0755)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	link := tmpDir + "/link"
	mnt := tmpDir + "/mnt"
	if err := os.Mkdir(mnt, 0755); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	if err := os.Symlink("orig", link); err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	fs := pathfs.NewLoopbackFileSystem(link)
	nfs := pathfs.NewPathNodeFs(fs, nil)
	state, _, err := nodefs.MountRoot(mnt, nfs.Root(), nil)
	if err != nil {
		t.Fatalf("MountNodeFileSystem failed: %v", err)
	}
	defer state.Unmount()

	go state.Serve()
	if err := state.WaitMount(); err != nil {
		t.Fatal("WaitMount", err)
	}

	if _, err := os.Lstat(mnt); err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
}

func TestDoubleOpen(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	tc.WriteFile(tc.orig+"/file", []byte("blabla"), 0644)

	roFile, err := os.Open(tc.mnt + "/file")
	if err != nil {
		t.Fatalf(" failed: %v", err)
	}
	defer roFile.Close()

	rwFile, err := os.OpenFile(tc.mnt+"/file", os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer rwFile.Close()
}

func TestUmask(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	// Make sure system setting does not affect test.
	fn := tc.mnt + "/file"
	mask := 020
	cmd := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("umask %o && mkdir %s", mask, fn))
	if err := cmd.Run(); err != nil {
		t.Fatalf("cmd.Run: %v", err)
	}

	fi, err := os.Lstat(fn)
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}

	expect := mask ^ 0777
	got := int(fi.Mode().Perm())
	if got != expect {
		t.Errorf("got %o, expect mode %o for file %s", got, expect, fn)
	}
}

// Check that chgrp(1) works
func TestChgrp(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Cleanup()

	f, err := os.Create(tc.mnt + "/file")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer f.Close()

	err = f.Chown(-1, os.Getgid())
	if err != nil {
		t.Errorf("Chown failed: %v", err)
	}
}
