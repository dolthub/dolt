package filesys

import (
	"github.com/pkg/errors"
	"io"
)

var ErrIsDir = errors.New("operation not valid on a directory")
var ErrIsFile = errors.New("operation not valid on a file")

// ReadableFS is an interface providing read access to objs in a filesystem
type ReadableFS interface {

	// OpenForRead opens a file for reading
	OpenForRead(fp string) (io.ReadCloser, error)

	// ReadFile reads the entire contents of a file
	ReadFile(fp string) ([]byte, error)

	// Exists will tell you if a file or directory with a given path already exists, and if it does is it a directory
	Exists(path string) (exists bool, isDir bool)
}

// WritableFS is an interface providing write access to objs in a filesystem
type WritableFS interface {

	// OpenForWrite opens a file for writing.  The file will be created if it does not exist, and if it does exist
	// it will be overwritten.
	OpenForWrite(fp string) (io.WriteCloser, error)

	// WriteFile writes the entire data buffer to a given file.  The file will be created if it does not exist,
	// and if it does exist it will be overwritten.
	WriteFile(fp string, data []byte) error

	// MkDirs creates a folder and all the parent folders that are necessary to create it.
	MkDirs(path string) error

	// DeleteFile will delete a file at the given path
	DeleteFile(path string) error

	// Delete will delete an empty directory, or a file.  If trying delete a directory that is not empty you can set force to
	// true in order to delete the dir and all of it's contents
	Delete(path string, force bool) error
}

// FSIterCB specifies the signature of the function that will be called for every item found while iterating.
type FSIterCB func(path string, size int64, isDir bool) (stop bool)

// WalkableFS is an interface for walking the files and subdirectories of a directory
type WalkableFS interface {

	// Iter iterates over the files and subdirectories within a given directory (Optionally recursively).  There
	// are no guarantees about the ordering of results.  Two calls to iterate the same directory may yield
	// differently ordered results.
	Iter(directory string, recursive bool, cb FSIterCB) error
}

// ReadWriteFS is an interface whose implementors will provide read, and write implementations but may not allow
// for files to be listed.
type ReadWriteFS interface {
	ReadableFS
	WritableFS
}

// Filesys is an interface whose implementors will provide read, write, and list mechanisms
type Filesys interface {
	ReadableFS
	WritableFS
	WalkableFS
}
