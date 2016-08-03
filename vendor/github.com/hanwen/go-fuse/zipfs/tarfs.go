package zipfs

import (
	"archive/tar"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"github.com/hanwen/go-fuse/fuse"
	"io"
	"os"
	"strings"
	"syscall"
)

// TODO - handle symlinks.

func HeaderToFileInfo(out *fuse.Attr, h *tar.Header) {
	out.Mode = uint32(h.Mode)
	out.Size = uint64(h.Size)
	out.Uid = uint32(h.Uid)
	out.Gid = uint32(h.Gid)
	out.SetTimes(&h.AccessTime, &h.ModTime, &h.ChangeTime)
}

type TarFile struct {
	data []byte
	tar.Header
}

func (f *TarFile) Stat(out *fuse.Attr) {
	HeaderToFileInfo(out, &f.Header)
	out.Mode |= syscall.S_IFREG
}

func (f *TarFile) Data() []byte {
	return f.data
}

func NewTarTree(r io.Reader) map[string]MemFile {
	files := map[string]MemFile{}
	tr := tar.NewReader(r)

	var longName *string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			// end of tar archive
			break
		}
		if err != nil {
			// handle error
		}

		if hdr.Typeflag == 'L' {
			buf := bytes.NewBuffer(make([]byte, 0, hdr.Size))
			io.Copy(buf, tr)
			s := buf.String()
			longName = &s
			continue
		}

		if longName != nil {
			hdr.Name = *longName
			longName = nil
		}

		if strings.HasSuffix(hdr.Name, "/") {
			continue
		}

		buf := bytes.NewBuffer(make([]byte, 0, hdr.Size))
		io.Copy(buf, tr)

		files[hdr.Name] = &TarFile{
			Header: *hdr,
			data:   buf.Bytes(),
		}
	}
	return files
}

func NewTarCompressedTree(name string, format string) (map[string]MemFile, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var stream io.Reader
	switch format {
	case "gz":
		unzip, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		defer unzip.Close()
		stream = unzip
	case "bz2":
		unzip := bzip2.NewReader(f)
		stream = unzip
	}

	return NewTarTree(stream), nil
}
