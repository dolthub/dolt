package tempfiles

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

// TempFileProvider is an interface which provides methods for creating temporary files.
type TempFileProvider interface {
	// GetTempDir returns the directory where temp files will be created by default
	GetTempDir() string

	// NewFile creates a new temporary file in the directory dir, opens the file for reading and writing, and returns
	// the resulting *os.File. If dir is "" then the default temp dir is used.
	NewFile(dir, pattern string) (*os.File, error)

	// Clean makes a best effort attempt to delete all temp files created by calls to NewFile
	Clean()
}

// TempFileProviderAt is a TempFileProvider interface which creates temp files at a given path.
type TempFileProviderAt struct {
	tempDir      string
	filesCreated []string
}

// NewTempFileProviderAt creates a new TempFileProviderAt instance with the provided directory to create files in. The
// directory is assumed to have been created already.
func NewTempFileProviderAt(tempDir string) *TempFileProviderAt {
	return &TempFileProviderAt{tempDir, nil}
}

// GetTempDir returns the directory where temp files will be created by default
func (tfp *TempFileProviderAt) GetTempDir() string {
	return tfp.tempDir
}

// NewFile creates a new temporary file in the directory dir, opens the file for reading and writing, and returns
// the resulting *os.File. If dir is "" then the default temp dir is used.
func (tfp *TempFileProviderAt) NewFile(dir, pattern string) (*os.File, error) {
	if !filepath.IsAbs(dir) {
		dir = filepath.Join(tfp.tempDir, dir)
	}

	f, err := ioutil.TempFile(dir, pattern)

	if err == nil {
		tfp.filesCreated = append(tfp.filesCreated, f.Name())
	}

	return f, err
}

// Clean makes a best effort attempt to delete all temp files created by calls to NewFile
func (tfp *TempFileProviderAt) Clean() {
	for _, filename := range tfp.filesCreated {
		// best effort. ignore errors
		_ = os.Remove(filename)
	}
}

// MovableTemFile is an object that implements TempFileProvider that is used by the nbs to create temp files that
// ultimately will be renamed.  It is important not to use this instance rather than using os.TempDir, or ioutil.TempFile
// directly as those may have errors executing a rename against if the volume the default temporary directory lives on
// is different than the volume of the destination of the rename.
var MovableTempFile TempFileProvider = NewTempFileProviderAt(os.TempDir())
