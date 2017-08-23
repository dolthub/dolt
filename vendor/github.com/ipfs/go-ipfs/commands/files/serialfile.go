package files

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// serialFile implements File, and reads from a path on the OS filesystem.
// No more than one file will be opened at a time (directories will advance
// to the next file when NextFile() is called).
type serialFile struct {
	name              string
	path              string
	files             []os.FileInfo
	stat              os.FileInfo
	current           *File
	handleHiddenFiles bool
}

func NewSerialFile(name, path string, hidden bool, stat os.FileInfo) (File, error) {

	switch mode := stat.Mode(); {
	case mode.IsRegular():
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return NewReaderPathFile(name, path, file, stat)
	case mode.IsDir():
		// for directories, stat all of the contents first, so we know what files to
		// open when NextFile() is called
		contents, err := ioutil.ReadDir(path)
		if err != nil {
			return nil, err
		}
		return &serialFile{name, path, contents, stat, nil, hidden}, nil
	case mode&os.ModeSymlink != 0:
		target, err := os.Readlink(path)
		if err != nil {
			return nil, err
		}
		return NewLinkFile(name, path, target, stat), nil
	default:
		return nil, fmt.Errorf("Unrecognized file type for %s: %s", name, mode.String())
	}
}

func (f *serialFile) IsDirectory() bool {
	// non-directories get created as a ReaderFile, so serialFiles should only
	// represent directories
	return true
}

func (f *serialFile) NextFile() (File, error) {
	// if a file was opened previously, close it
	err := f.Close()
	if err != nil {
		return nil, err
	}

	// if there aren't any files left in the root directory, we're done
	if len(f.files) == 0 {
		return nil, io.EOF
	}

	stat := f.files[0]
	f.files = f.files[1:]

	for !f.handleHiddenFiles && strings.HasPrefix(stat.Name(), ".") {
		if len(f.files) == 0 {
			return nil, io.EOF
		}

		stat = f.files[0]
		f.files = f.files[1:]
	}

	// open the next file
	fileName := filepath.ToSlash(filepath.Join(f.name, stat.Name()))
	filePath := filepath.ToSlash(filepath.Join(f.path, stat.Name()))

	// recursively call the constructor on the next file
	// if it's a regular file, we will open it as a ReaderFile
	// if it's a directory, files in it will be opened serially
	sf, err := NewSerialFile(fileName, filePath, f.handleHiddenFiles, stat)
	if err != nil {
		return nil, err
	}

	f.current = &sf

	return sf, nil
}

func (f *serialFile) FileName() string {
	return f.name
}

func (f *serialFile) FullPath() string {
	return f.path
}

func (f *serialFile) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (f *serialFile) Close() error {
	// close the current file if there is one
	if f.current != nil {
		err := (*f.current).Close()
		// ignore EINVAL error, the file might have already been closed
		if err != nil && err != syscall.EINVAL {
			return err
		}
	}

	return nil
}

func (f *serialFile) Stat() os.FileInfo {
	return f.stat
}

func (f *serialFile) Size() (int64, error) {
	if !f.stat.IsDir() {
		return f.stat.Size(), nil
	}

	var du int64
	err := filepath.Walk(f.FullPath(), func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if fi != nil && fi.Mode()&(os.ModeSymlink|os.ModeNamedPipe) == 0 {
			du += fi.Size()
		}
		return nil
	})

	return du, err
}
