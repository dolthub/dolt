package chunks

import (
	"bytes"
	"flag"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type FileStore struct {
	dir, root string

	// For testing
	mkdirAll mkdirAllFn
}

type mkdirAllFn func(path string, perm os.FileMode) error

func NewFileStore(dir, root string) FileStore {
	d.Chk.NotEmpty(dir)
	d.Chk.NotEmpty(root)
	d.Chk.NoError(os.MkdirAll(dir, 0700))
	return FileStore{dir, path.Join(dir, root), os.MkdirAll}
}

func readRef(file *os.File) ref.Ref {
	s, err := ioutil.ReadAll(file)
	d.Chk.NoError(err)
	if len(s) == 0 {
		return ref.Ref{}
	}

	return ref.MustParse(string(s))
}

func (f FileStore) Root() ref.Ref {
	file, err := os.Open(f.root)
	if os.IsNotExist(err) {
		return ref.Ref{}
	}
	d.Chk.NoError(err)

	syscall.Flock(int(file.Fd()), syscall.LOCK_SH)
	defer file.Close()

	return readRef(file)
}

func (f FileStore) UpdateRoot(current, last ref.Ref) bool {
	file, err := os.OpenFile(f.root, os.O_RDWR|os.O_CREATE, os.ModePerm)
	d.Chk.NoError(err)
	syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
	defer file.Close()

	existing := readRef(file)
	if existing != last {
		return false
	}

	file.Seek(0, 0)
	file.Truncate(0)
	file.Write([]byte(current.String()))
	return true
}

func (f FileStore) Get(ref ref.Ref) (io.ReadCloser, error) {
	r, err := os.Open(getPath(f.dir, ref))
	if os.IsNotExist(err) {
		return nil, nil
	}
	return r, err
}

func (f FileStore) Put() ChunkWriter {
	b := &bytes.Buffer{}
	h := ref.NewHash()
	return &fileChunkWriter{
		root:     f.dir,
		buffer:   b,
		writer:   io.MultiWriter(b, h),
		hash:     h,
		mkdirAll: f.mkdirAll,
	}
}

type fileChunkWriter struct {
	root     string
	buffer   *bytes.Buffer
	writer   io.Writer
	hash     hash.Hash
	mkdirAll mkdirAllFn
}

func (w *fileChunkWriter) Write(data []byte) (int, error) {
	d.Chk.NotNil(w.buffer, "Write() cannot be called after Ref() or Close().")
	return w.writer.Write(data)
}

func (w *fileChunkWriter) Ref() (ref.Ref, error) {
	d.Chk.NoError(w.Close())
	return ref.FromHash(w.hash), nil
}

func (w *fileChunkWriter) Close() error {
	if w.buffer == nil {
		return nil
	}

	p := getPath(w.root, ref.FromHash(w.hash))

	// If we already have this file, then nothing to do. Hooray.
	if _, err := os.Stat(p); err == nil {
		return nil
	}

	err := w.mkdirAll(path.Dir(p), 0700)
	d.Chk.NoError(err)

	file, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, os.ModePerm)
	defer file.Close()
	if err != nil {
		d.Chk.True(os.IsExist(err), "%+v\n", err)
	}

	totalBytes := w.buffer.Len()
	written, err := io.Copy(file, w.buffer)
	d.Chk.NoError(err)
	d.Chk.True(int64(totalBytes) == written, "Too few bytes written.") // BUG #83

	w.buffer = nil
	return nil
}

func getPath(root string, ref ref.Ref) string {
	s := ref.String()
	d.Chk.True(strings.HasPrefix(s, "sha1"))
	return path.Join(root, "sha1", s[5:7], s[7:9], s)
}

type fileStoreFlags struct {
	dir  *string
	root *string
}

func fileFlags(prefix string) fileStoreFlags {
	return fileStoreFlags{
		flag.String(prefix+"fs", "", "directory to use for a file-based chunkstore"),
		flag.String(prefix+"fs-root", "root", "filename which holds the root ref in the filestore"),
	}
}

func (f fileStoreFlags) createStore() ChunkStore {
	if *f.dir == "" || *f.root == "" {
		return nil
	} else {
		fs := NewFileStore(*f.dir, *f.root)
		return &fs
	}
}
