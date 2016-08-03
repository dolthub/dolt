package unionfs

import (
	"io/ioutil"
	"os"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

func modeMapEq(m1, m2 map[string]uint32) bool {
	if len(m1) != len(m2) {
		return false
	}

	for k, v := range m1 {
		val, ok := m2[k]
		if !ok || val != v {
			return false
		}
	}
	return true
}

func TestCachingFs(t *testing.T) {
	wd, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(wd)

	fs := pathfs.NewLoopbackFileSystem(wd)
	cfs := NewCachingFileSystem(fs, 0)

	os.Mkdir(wd+"/orig", 0755)
	fi, code := cfs.GetAttr("orig", nil)
	if !code.Ok() {
		t.Fatal("GetAttr failure", code)
	}
	if !fi.IsDir() {
		t.Error("unexpected attr", fi)
	}

	os.Symlink("orig", wd+"/symlink")

	val, code := cfs.Readlink("symlink", nil)
	if val != "orig" {
		t.Error("unexpected readlink", val)
	}
	if !code.Ok() {
		t.Error("code !ok ", code)
	}

	stream, code := cfs.OpenDir("", nil)
	if !code.Ok() {
		t.Fatal("Readdir fail", code)
	}

	results := make(map[string]uint32)
	for _, v := range stream {
		results[v.Name] = v.Mode &^ 07777
	}
	expected := map[string]uint32{
		"symlink": syscall.S_IFLNK,
		"orig":    fuse.S_IFDIR,
	}
	if !modeMapEq(results, expected) {
		t.Error("Unexpected readdir result", results, expected)
	}
}
