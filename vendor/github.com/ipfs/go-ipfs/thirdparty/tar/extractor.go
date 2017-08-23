package tar

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	gopath "path"
	fp "path/filepath"
	"strings"
)

type Extractor struct {
	Path     string
	Progress func(int64) int64
}

func (te *Extractor) Extract(reader io.Reader) error {
	tarReader := tar.NewReader(reader)

	// Check if the output path already exists, so we know whether we should
	// create our output with that name, or if we should put the output inside
	// a preexisting directory
	rootExists := true
	rootIsDir := false
	if stat, err := os.Stat(te.Path); err != nil && os.IsNotExist(err) {
		rootExists = false
	} else if err != nil {
		return err
	} else if stat.IsDir() {
		rootIsDir = true
	}

	// files come recursively in order (i == 0 is root directory)
	for i := 0; ; i++ {
		header, err := tarReader.Next()
		if err != nil && err != io.EOF {
			return err
		}
		if header == nil || err == io.EOF {
			break
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := te.extractDir(header, i); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := te.extractFile(header, tarReader, i, rootExists, rootIsDir); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := te.extractSymlink(header); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognized tar header type: %d", header.Typeflag)
		}
	}
	return nil
}

// outputPath returns the path at whicht o place tarPath
func (te *Extractor) outputPath(tarPath string) string {
	elems := strings.Split(tarPath, "/") // break into elems
	elems = elems[1:]                    // remove original root

	path := fp.Join(elems...)     // join elems
	path = fp.Join(te.Path, path) // rebase on extractor root
	return path
}

func (te *Extractor) extractDir(h *tar.Header, depth int) error {
	path := te.outputPath(h.Name)

	if depth == 0 {
		// if this is the root root directory, use it as the output path for remaining files
		te.Path = path
	}

	return os.MkdirAll(path, 0755)
}

func (te *Extractor) extractSymlink(h *tar.Header) error {
	return os.Symlink(h.Linkname, te.outputPath(h.Name))
}

func (te *Extractor) extractFile(h *tar.Header, r *tar.Reader, depth int, rootExists bool, rootIsDir bool) error {
	path := te.outputPath(h.Name)

	if depth == 0 { // if depth is 0, this is the only file (we aren't 'ipfs get'ing a directory)
		if rootExists && rootIsDir {
			// putting file inside of a root dir.
			fnameo := gopath.Base(h.Name)
			fnamen := fp.Base(path)
			// add back original name if lost.
			if fnameo != fnamen {
				path = fp.Join(path, fnameo)
			}
		} // else if old file exists, just overwrite it.
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return copyWithProgress(file, r, te.Progress)
}

func copyWithProgress(to io.Writer, from io.Reader, cb func(int64) int64) error {
	buf := make([]byte, 4096)
	for {
		n, err := from.Read(buf)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		cb(int64(n))
		_, err = to.Write(buf[:n])
		if err != nil {
			return err
		}
	}

}
