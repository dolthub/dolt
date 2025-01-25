// Copyright 2019 Dolthub, Inc.
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
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/osutil"
	"github.com/dolthub/dolt/go/libraries/utils/test"
)

const (
	testFilename       = "testfile.txt"
	testSubdirFilename = "anothertest.txt"
	movedFilename      = "movedfile.txt"
	testString         = "this is a test"
	testStringLen      = int64(len(testString))
)

var filesysetmsToTest = map[string]Filesys{
	"inmem": EmptyInMemFS("/"),
	"local": LocalFS,
}

func TestFilesystems(t *testing.T) {
	dir := test.TestDir(t.TempDir(), "filesys_test")
	newLocation := test.TestDir(t.TempDir(), "newLocation")
	subdir := filepath.Join(dir, "subdir")
	subdirFile := filepath.Join(subdir, testSubdirFilename)
	fp := filepath.Join(dir, testFilename)
	movedFilePath := filepath.Join(dir, movedFilename)

	for fsName, fs := range filesysetmsToTest {
		t.Run(fsName, func(t *testing.T) {
			// Test file doesn't exist before creation
			exists, _ := fs.Exists(dir)
			require.False(t, exists)

			// Test creating directories
			err := fs.MkDirs(dir)
			require.NoError(t, err)
			err = fs.MkDirs(subdir)
			require.NoError(t, err)

			// Test directories exists, and are in fact directories
			exists, isDir := fs.Exists(dir)
			require.True(t, exists)
			require.True(t, isDir)
			exists, isDir = fs.Exists(subdir)
			require.True(t, exists)
			require.True(t, isDir)

			// Test failure to open a directory for read
			_, err = fs.OpenForRead(dir)
			require.Error(t, err)

			// Test failure to open a directory for write
			_, err = fs.OpenForWrite(dir, os.ModePerm)
			require.Error(t, err)

			// Test file doesn't exist before creation
			exists, _ = fs.Exists(fp)
			require.False(t, exists)

			// Test can't open a file that doesn't exist for read
			_, err = fs.OpenForRead(fp)
			require.Error(t, err)

			data := test.RandomData(256 * 1024)

			// Test writing file with random data
			err = fs.WriteFile(fp, data, os.ModePerm)
			require.NoError(t, err)

			// Test that the data can be read back and hasn't changed
			dataRead, err := fs.ReadFile(fp)
			require.NoError(t, err)
			require.Equal(t, dataRead, data)

			// Test moving the file
			err = fs.MoveFile(fp, movedFilePath)
			require.NoError(t, err)

			// Test that there is no longer a file at the initial path
			exists, _ = fs.Exists(fp)
			require.False(t, exists)

			// Test that a file exists at the new location
			exists, isDir = fs.Exists(movedFilePath)
			require.True(t, exists)
			require.False(t, isDir)

			// Test that the data can be read back and hasn't changed since being moved
			dataRead, err = fs.ReadFile(movedFilePath)
			require.NoError(t, err)
			require.Equal(t, dataRead, data)

			tmp := fs.TempDir()
			require.NotEmpty(t, tmp)
			fp2 := filepath.Join(tmp, "data.txt")
			wrc, err := fs.OpenForWrite(fp2, os.ModePerm)
			require.NoError(t, err)
			require.NoError(t, wrc.Close())

			// Test moving a directory
			err = fs.WriteFile(subdirFile, []byte("helloworld"), os.ModePerm)
			require.NoError(t, err)
			err = fs.MkDirs(newLocation)
			require.NoError(t, err)
			err = fs.MoveDir(subdir, filepath.Join(newLocation, "subdir"))
			require.NoError(t, err)

			// Assert that nothing exists at the old path
			exists, isDir = fs.Exists(subdir)
			require.False(t, exists)
			require.False(t, isDir)

			// Assert that our directory exists at the new path
			exists, isDir = fs.Exists(filepath.Join(newLocation, "subdir"))
			require.True(t, exists)
			require.True(t, isDir)

			// Assert that the file in the sub-directory has been moved, too
			exists, isDir = fs.Exists(subdirFile)
			require.False(t, exists)
			require.False(t, isDir)
			exists, isDir = fs.Exists(filepath.Join(newLocation, "subdir", testSubdirFilename))
			require.True(t, exists)
			require.False(t, isDir)

			// Test writing/reading random data to tmp file
			err = fs.WriteFile(fp2, data, os.ModePerm)
			require.NoError(t, err)
			dataRead, err = fs.ReadFile(fp2)
			require.NoError(t, err)
			require.Equal(t, dataRead, data)
		})
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
	dir := test.TestDir(t.TempDir(), "TestRecursiveFSIteration")

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
	dir := test.TestDir(t.TempDir(), "TestFSIteration")

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
	dir := test.TestDir(t.TempDir(), "TestDeletes")

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
	fs.WriteFile(fp, []byte(testString), os.ModePerm)
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
