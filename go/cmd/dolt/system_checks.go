package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/liquidata-inc/dolt/go/store/util/tempfiles"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
)

// returns false if it fails to verify that it can move files from the default temp directory to the local directory.
func canMoveTempFile() bool {
	const testfile = "./testfile"

	f, err := ioutil.TempFile("", "")

	if err != nil {
		return false
	}

	name := f.Name()
	err = f.Close()

	if err != nil {
		return false
	}

	err = os.Rename(name, testfile)

	if err != nil {
		_ = os.Remove(name)
		return false
	}

	_ = os.Remove(testfile)
	return true
}

// If we cannot verify that we can move files for any reason, use a ./.dolt/tmp as the temp dir.
func reconfigIfTempFileMoveFails(dEnv *env.DoltEnv) error {
	if !canMoveTempFile() {
		tmpDir := "./.dolt/tmp"

		if !dEnv.HasDoltDir() {
			tmpDir = "./.tmp"
		}

		stat, err := os.Stat(tmpDir)

		if err != nil {
			err := os.MkdirAll(tmpDir, os.ModePerm)

			if err != nil {
				return fmt.Errorf("failed to create temp dir '%s': %s", tmpDir, err.Error())
			}
		} else if !stat.IsDir() {
			return fmt.Errorf("attempting to use '%s' as a temp directory, but there exists a file with that name", tmpDir)
		}

		tempfiles.MovableTempFile = tempfiles.NewTempFileProviderAt(tmpDir)
	}

	return nil
}
