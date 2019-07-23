package test

import (
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

// TestDir creates a subdirectory inside the systems temp directory
func TestDir(testName string) string {
	id, err := uuid.NewRandom()

	if err != nil {
		panic(ShouldNeverHappen)
	}

	return filepath.Join(os.TempDir(), testName, id.String())
}

// ChangeToTestDir creates a new test directory and changes the current directory to be
func ChangeToTestDir(testName string) (string, error) {
	dir := TestDir(testName)
	err := os.MkdirAll(dir, os.ModePerm)

	if err != nil {
		return "", err
	}

	err = os.Chdir(dir)

	if err != nil {
		return "", err
	}

	return dir, nil
}
