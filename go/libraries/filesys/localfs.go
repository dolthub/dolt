package filesys

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

// LocalFS is the machines local filesystem
var LocalFS = &localFS{}

type localFS struct{}

// Exists will tell you if a file or directory with a given path already exists, and if it does is it a directory
func (fs *localFS) Exists(path string) (exists bool, isDir bool) {
	stat, err := os.Stat(path)

	if err != nil {
		return false, false
	}

	return true, stat.IsDir()
}

var errStopMarker = errors.New("stop")

// Iter iterates over the files and subdirectories within a given directory (Optionally recursively.
func (fs *localFS) Iter(path string, recursive bool, cb FSIterCB) error {
	if !recursive {
		info, err := ioutil.ReadDir(path)

		if err != nil {
			return err
		}

		for _, curr := range info {
			stop := cb(filepath.Join(path, curr.Name()), curr.Size(), curr.IsDir())

			if stop {
				return nil
			}
		}

		return nil
	}

	return fs.iter(path, cb)
}

func (fs *localFS) iter(dir string, cb FSIterCB) error {
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if dir != path {
			stop := cb(path, info.Size(), info.IsDir())

			if stop {
				return errStopMarker
			}
		}
		return nil
	})

	if err == errStopMarker {
		return nil
	}

	return err
}

// OpenForRead opens a file for reading
func (fs *localFS) OpenForRead(fp string) (io.ReadCloser, error) {
	if exists, isDir := fs.Exists(fp); !exists {
		return nil, errors.New("File does not exist.")
	} else if isDir {
		return nil, errors.New(fp + " is a directory and can't be opened for reading.")
	}

	return os.Open(fp)
}

// ReadFile reads the entire contents of a file
func (fs *localFS) ReadFile(fp string) ([]byte, error) {
	return ioutil.ReadFile(fp)
}

// OpenForWrite opens a file for writing.  The file will be created if it does not exist, and if it does exist
// it will be overwritten.
func (fs *localFS) OpenForWrite(fp string) (io.WriteCloser, error) {
	return os.OpenFile(fp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)
}

// WriteFile writes the entire data buffer to a given file.  The file will be created if it does not exist,
// and if it does exist it will be overwritten.
func (fs *localFS) WriteFile(fp string, data []byte) error {
	return ioutil.WriteFile(fp, data, os.ModePerm)
}

// MkDirs creates a folder and all the parent folders that are necessary to create it.
func (fs *localFS) MkDirs(path string) error {
	_, err := os.Stat(path)

	if err != nil {
		return os.MkdirAll(path, os.ModePerm)
	}

	return nil
}

// DeleteFile will delete a file at the given path
func (fs *localFS) DeleteFile(path string) error {
	if exists, isDir := fs.Exists(path); exists && !isDir {
		if isDir {
			return errors.New(path + " is a directory not a file.")
		}

		return os.Remove(path)
	}

	return errors.New(path + " not found in filesystem.")
}

// Delete will delete an empty directory, or a file.  If trying delete a directory that is not empty you can set force to
// true in order to delete the dir and all of it's contents
func (fs *localFS) Delete(path string, force bool) error {
	if !force {
		return os.Remove(path)
	} else {
		return os.RemoveAll(path)
	}
}
