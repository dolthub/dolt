package http

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/textproto"
	"net/url"
	"sync"

	files "github.com/ipfs/go-ipfs/commands/files"
)

// MultiFileReader reads from a `commands.File` (which can be a directory of files
// or a regular file) as HTTP multipart encoded data.
type MultiFileReader struct {
	io.Reader

	files       []files.File
	currentFile io.Reader
	buf         bytes.Buffer
	mpWriter    *multipart.Writer
	closed      bool
	mutex       *sync.Mutex

	// if true, the data will be type 'multipart/form-data'
	// if false, the data will be type 'multipart/mixed'
	form bool
}

// NewMultiFileReader constructs a MultiFileReader. `file` can be any `commands.File`.
// If `form` is set to true, the multipart data will have a Content-Type of 'multipart/form-data',
// if `form` is false, the Content-Type will be 'multipart/mixed'.
func NewMultiFileReader(file files.File, form bool) *MultiFileReader {
	mfr := &MultiFileReader{
		files: []files.File{file},
		form:  form,
		mutex: &sync.Mutex{},
	}
	mfr.mpWriter = multipart.NewWriter(&mfr.buf)

	return mfr
}

func (mfr *MultiFileReader) Read(buf []byte) (written int, err error) {
	mfr.mutex.Lock()
	defer mfr.mutex.Unlock()

	// if we are closed and the buffer is flushed, end reading
	if mfr.closed && mfr.buf.Len() == 0 {
		return 0, io.EOF
	}

	// if the current file isn't set, advance to the next file
	if mfr.currentFile == nil {
		var file files.File
		for file == nil {
			if len(mfr.files) == 0 {
				mfr.mpWriter.Close()
				mfr.closed = true
				return mfr.buf.Read(buf)
			}

			nextfile, err := mfr.files[len(mfr.files)-1].NextFile()
			if err == io.EOF {
				mfr.files = mfr.files[:len(mfr.files)-1]
				continue
			} else if err != nil {
				return 0, err
			}

			file = nextfile
		}

		// handle starting a new file part
		if !mfr.closed {

			var contentType string
			if _, ok := file.(*files.Symlink); ok {
				contentType = "application/symlink"
			} else if file.IsDirectory() {
				mfr.files = append(mfr.files, file)
				contentType = "application/x-directory"
			} else {
				// otherwise, use the file as a reader to read its contents
				contentType = "application/octet-stream"
			}

			mfr.currentFile = file

			// write the boundary and headers
			header := make(textproto.MIMEHeader)
			filename := url.QueryEscape(file.FileName())
			header.Set("Content-Disposition", fmt.Sprintf("file; filename=\"%s\"", filename))

			header.Set("Content-Type", contentType)
			if rf, ok := file.(*files.ReaderFile); ok {
				header.Set("abspath", rf.AbsPath())
			}

			_, err := mfr.mpWriter.CreatePart(header)
			if err != nil {
				return 0, err
			}
		}
	}

	// if the buffer has something in it, read from it
	if mfr.buf.Len() > 0 {
		return mfr.buf.Read(buf)
	}

	// otherwise, read from file data
	written, err = mfr.currentFile.Read(buf)
	if err == io.EOF {
		mfr.currentFile = nil
		return written, nil
	}
	return written, err
}

// Boundary returns the boundary string to be used to separate files in the multipart data
func (mfr *MultiFileReader) Boundary() string {
	return mfr.mpWriter.Boundary()
}
