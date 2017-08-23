package osrename_test

import (
	"bytes"
	rn "gx/ipfs/QmaeRR9SpXumU5tYLRkq6x6pfMe8qKzxn4ujBpsTJ2zQG7/go-os-rename"
	"io/ioutil"
	"os"
	"testing"
)

func tempdir(t testing.TB) (path string, cleanup func()) {
	path, err := ioutil.TempDir("", "test-windows-rename")
	if err != nil {
		t.Fatalf("cannot create temp directory: %v", err)
	}

	cleanup = func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("tempdir cleanup failed: %v", err)
		}
	}
	return path, cleanup
}

func TestAtomicRename(t *testing.T) {
	dirBase, cleanup := tempdir(t)
	defer cleanup()

	// Create base file
	origFilePath := dirBase + "original.txt"
	err := ioutil.WriteFile(origFilePath, []byte("tests"), 0644)

	if err != nil {
		t.Fatalf("Could not write original test file")
	}

	// Create secondary file
	tempFilePath := dirBase + "newTempFile.txt"
	err = ioutil.WriteFile(tempFilePath, []byte("success"), 0644)
	if err != nil {
		t.Fatalf("Could not write temp file")
	}

	// Execute our magic rename function
	err = rn.Rename(tempFilePath, origFilePath)
	if err != nil {
		t.Fatalf("Could not rename temp file")
	}

	// Let's read the renamed file and ensure that we get data
	renamedFileBytes, err := ioutil.ReadFile(origFilePath)
	if err != nil {
		t.Fatalf("Could not read renamed file")
	}

	// Let's compare the bytes of the renamed file
	if bytes.Compare(renamedFileBytes, []byte("success")) != 0 {
		t.Fatalf("Did not find expected bytes in renamed file %d vs %d", renamedFileBytes, []byte("success"))
	}
}
