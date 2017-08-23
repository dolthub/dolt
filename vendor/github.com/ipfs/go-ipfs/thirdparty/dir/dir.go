package dir

// TODO move somewhere generic

import (
	"errors"
	"os"
	"path/filepath"
)

// Writable ensures the directory exists and is writable
func Writable(path string) error {
	// Construct the path if missing
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return err
	}
	// Check the directory is writeable
	if f, err := os.Create(filepath.Join(path, "._check_writeable")); err == nil {
		f.Close()
		os.Remove(f.Name())
	} else {
		return errors.New("'" + path + "' is not writeable")
	}
	return nil
}
