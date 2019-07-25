// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package filesys

import (
	"bytes"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/liquidata-inc/dolt/go/libraries/utils/osutil"
	"github.com/liquidata-inc/dolt/go/libraries/utils/test"
)

const (
	testFilename  = "testfile.txt"
	testString    = "this is a test"
	testStringLen = int64(len(testString))
)

var filesysetmsToTest = map[string]Filesys{
	"inmem": EmptyInMemFS("/"),
	"local": LocalFS,
}

func TestFilesystems(t *testing.T) {
	dir := test.TestDir("filesys_test")
	fp := filepath.Join(dir, testFilename)

	for fsName, fs := range filesysetmsToTest {
		if exists, _ := fs.Exists(dir); exists {
			t.Error("fs:", fsName, "Directory existed before it was created:", dir)
			continue
		}

		err := fs.MkDirs(dir)

		if err != nil {
			t.Error("fs:", fsName, "failed to make dir", dir, err)
			continue
		}

		if exists, isDir := fs.Exists(dir); !exists || !isDir {
			t.Error("fs:", fsName, "Directory not found after creating:", dir)
			continue
		}

		_, err = fs.OpenForRead(dir)

		if err == nil {
			t.Error("fs:", fsName, "shouldn't be able to open a directory for reading")
			continue
		}

		_, err = fs.OpenForWrite(dir)

		if err == nil {
			t.Error("fs:", fsName, "shouldn't be able to open a directory for writing")
			continue
		}

		if exists, _ := fs.Exists(fp); exists {
			t.Error("fs:", fsName, "file existed before creating:", fp)
			continue
		}

		_, err = fs.OpenForRead(fp)

		if err == nil {
			t.Error("fs:", fsName, "Shouldn't be able to read a file that isn't there")
			continue
		}

		data := test.RandomData(256 * 1024)
		err = fs.WriteFile(fp, data)

		if err != nil {
			t.Error("fs:", fsName, "failed to write file", fp, err)
			continue
		}

		dataRead, err := fs.ReadFile(fp)

		if err != nil {
			t.Error("fs:", fsName, "failed to read the data that was written", fp, err)
			continue
		}

		if !bytes.Equal(dataRead, data) {
			t.Error("fs:", fsName, "data read does not match what was written", fp)
			continue
		}
	}
}

func TestNewInMemFS(t *testing.T) {
	fs := NewInMemFS([]string{"/r1/c1", "r2/c1/gc1"}, map[string][]byte{
		"/r1/c1/file1.txt": []byte(testString),
		"/r3/file2.txt":    []byte(testString),
	}, "/")

	expectedDirs := []string{
		osutil.PathToNative("/r1"),
		osutil.PathToNative("/r1/c1"),
		osutil.PathToNative("/r2"),
		osutil.PathToNative("/r2/c1"),
		osutil.PathToNative("/r2/c1/gc1"),
		osutil.PathToNative("/r3"),
	}

	expectedFiles := []string{
		osutil.PathToNative("/r1/c1/file1.txt"),
		osutil.PathToNative("/r3/file2.txt"),
	}

	actualDirs, actualFiles, err := iterate(fs, "/", true, t)

	if err != nil {
		t.Error("Error iterating")
	}

	validate(expectedDirs, expectedFiles, actualDirs, actualFiles, "inmem", t)
}

func TestRecursiveFSIteration(t *testing.T) {
	dir := test.TestDir("TestRecursiveFSIteration")

	for fsName, fs := range filesysetmsToTest {
		var expectedDirs []string
		var expectedFiles []string

		expectedDirs = makeDirsAddExpected(expectedDirs, fs, dir, "child1")
		expectedDirs = makeDirsAddExpected(expectedDirs, fs, dir, "child2", "grandchild1")
		expectedDirs = makeDirsAddExpected(expectedDirs, fs, dir, "child3", "grandchild2")
		expectedDirs = makeDirsAddExpected(expectedDirs, fs, filepath.Join(dir, "child3"), "grandchild3")

		expectedFiles = writeFileAddToExp(expectedFiles, fs, dir, "child1", "File1.txt")
		expectedFiles = writeFileAddToExp(expectedFiles, fs, dir, "child2", "grandchild1", "File1.txt")
		expectedFiles = writeFileAddToExp(expectedFiles, fs, dir, "child3", "grandchild2", "File1.txt")
		expectedFiles = writeFileAddToExp(expectedFiles, fs, dir, "child3", "grandchild2", "File2.txt")
		expectedFiles = writeFileAddToExp(expectedFiles, fs, dir, "child3", "grandchild3", "File1.txt")

		actualDirs, actualFiles, err := iterate(fs, dir, true, t)

		if err != nil {
			t.Error("fs:", fsName, "Failed to iterate.", err.Error())
			continue
		}

		validate(expectedDirs, expectedFiles, actualDirs, actualFiles, fsName, t)
	}
}

func TestFSIteration(t *testing.T) {
	dir := test.TestDir("TestFSIteration")

	for fsName, fs := range filesysetmsToTest {
		var expectedDirs []string
		var expectedFiles []string
		var ignored []string

		makeDirsAddExpected(ignored, fs, dir, "child1")
		makeDirsAddExpected(ignored, fs, dir, "child2", "grandchild1")
		makeDirsAddExpected(ignored, fs, dir, "child3")

		child3path := filepath.Join(dir, "child3")
		expectedDirs = makeDirsAddExpected(expectedDirs, fs, child3path, "grandchild2")
		expectedDirs = makeDirsAddExpected(expectedDirs, fs, child3path, "grandchild3")
		expectedFiles = writeFileAddToExp(expectedFiles, fs, child3path, "File1.txt")

		writeFileAddToExp(ignored, fs, dir, "child1", "File1.txt")
		writeFileAddToExp(ignored, fs, dir, "child2", "grandchild1", "File1.txt")
		writeFileAddToExp(ignored, fs, dir, "child3", "grandchild2", "File1.txt")
		writeFileAddToExp(ignored, fs, dir, "child3", "grandchild2", "File2.txt")
		writeFileAddToExp(ignored, fs, dir, "child3", "grandchild3", "File1.txt")

		actualDirs, actualFiles, err := iterate(fs, filepath.Join(dir, "child3"), false, t)

		if err != nil {
			t.Error("fs:", fsName, "Failed to iterate.", err.Error())
			continue
		}

		validate(expectedDirs, expectedFiles, actualDirs, actualFiles, fsName, t)
	}
}

func TestDeletes(t *testing.T) {
	dir := test.TestDir("TestDeletes")

	for fsName, fs := range filesysetmsToTest {
		var ignored []string

		makeDirsAddExpected(ignored, fs, dir, "child1")
		makeDirsAddExpected(ignored, fs, dir, "child2", "grandchild1")
		makeDirsAddExpected(ignored, fs, dir, "child3")

		writeFileAddToExp(ignored, fs, dir, "child1", "File1.txt")
		writeFileAddToExp(ignored, fs, dir, "child2", "grandchild1", "File1.txt")
		writeFileAddToExp(ignored, fs, dir, "child3", "grandchild2", "File1.txt")
		writeFileAddToExp(ignored, fs, dir, "child3", "grandchild2", "File2.txt")
		writeFileAddToExp(ignored, fs, dir, "child3", "grandchild3", "File1.txt")

		var err error
		err = fs.Delete(filepath.Join(dir, "child1"), false)

		if err == nil {
			t.Error("fs:", fsName, "Should have failed to delete non empty directory without force flag")
		}

		err = fs.DeleteFile(filepath.Join(dir, "child1", "File1.txt"))

		if err != nil {
			t.Error("fs:", fsName, "Should have succeeded to delete file")
		}

		err = fs.DeleteFile(filepath.Join(dir, "child1"))

		if err == nil {
			t.Error("fs:", fsName, "DeleteFile should not delete directories")
		}

		err = fs.Delete(filepath.Join(dir, "child1"), false)

		if err != nil {
			t.Error("fs:", fsName, "Should have succeeded to delete empty directory")
		}

		err = fs.Delete(filepath.Join(dir, "child2"), true)

		if err != nil {
			t.Error("fs:", fsName, "Should have succeeded to force delete directory and it")
		}
	}

}

func makeDirsAddExpected(expected []string, fs Filesys, root string, descendants ...string) []string {
	currDir := root
	for _, descendant := range descendants {
		currDir = filepath.Join(currDir, descendant)
		expected = append(expected, currDir)
	}

	err := fs.MkDirs(currDir)

	if err != nil {
		panic("failed to make dir")
	}

	return expected
}

func writeFileAddToExp(expected []string, fs Filesys, root string, pathFromRoot ...string) []string {
	pathElements := append([]string{root}, pathFromRoot...)

	fp := filepath.Join(pathElements...)
	fs.WriteFile(fp, []byte(testString))
	return append(expected, fp)
}

func iterate(fs Filesys, dir string, recursive bool, t *testing.T) ([]string, []string, error) {
	actualDirs := make([]string, 0, 10)
	actualFiles := make([]string, 0, 10)
	err := fs.Iter(dir, recursive, func(path string, size int64, isDir bool) (stop bool) {
		if isDir {
			actualDirs = append(actualDirs, path)
		} else {
			actualFiles = append(actualFiles, path)

			if size != testStringLen {
				t.Error(path, "is not of the expected size.")
			}
		}

		return false
	})

	return actualDirs, actualFiles, err
}

func validate(expectedDirs, expectedFiles, actualDirs, actualFiles []string, fsName string, t *testing.T) {
	sort.Strings(expectedDirs)
	sort.Strings(expectedFiles)
	sort.Strings(actualDirs)
	sort.Strings(actualFiles)

	if !reflect.DeepEqual(expectedDirs, actualDirs) {
		t.Error("fs:", fsName, "Expected dirs does not match actual dirs.", "\n\tactual  :", actualDirs, "\n\texpected:", expectedDirs)
	}

	if !reflect.DeepEqual(expectedFiles, actualFiles) {
		t.Error("fs:", fsName, "Expected files does not match actual files.", "\n\tactual  :", actualFiles, "\n\texpected:", expectedFiles)
	}
}
