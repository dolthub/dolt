package chunks

import (
	"flag"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"syscall"

	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

var (
	dirFlag  = flag.String("file-store", "", "directory to use as a FileStore")
	rootFlag = flag.String("root", "root", "file which holds root ref")
)

type FileStore struct {
	dir, root string
}

func NewFileStore(dir, root string) FileStore {
	Chk.NotEmpty(dir)
	Chk.NotEmpty(root)
	Chk.NoError(os.MkdirAll(dir, 0700))
	return FileStore{dir, path.Join(dir, root)}
}

func NewFileStoreFromFlags() FileStore {
	return NewFileStore(*dirFlag, *rootFlag)
}

func readRef(file *os.File) ref.Ref {
	s, err := ioutil.ReadAll(file)
	Chk.NoError(err)
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
	Chk.NoError(err)

	syscall.Flock(int(file.Fd()), syscall.LOCK_SH)
	defer file.Close()

	return readRef(file)
}

func (f FileStore) UpdateRoot(current, last ref.Ref) bool {
	file, err := os.OpenFile(f.root, os.O_RDWR|os.O_CREATE, os.ModePerm)
	Chk.NoError(err)
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
	return os.Open(getPath(f.dir, ref))
}

func (f FileStore) Put() ChunkWriter {
	return &fileChunkWriter{
		root: f.dir,
		hash: ref.NewHash(),
	}
}

type fileChunkWriter struct {
	root   string
	file   *os.File
	writer io.Writer
	hash   hash.Hash
}

func (w *fileChunkWriter) Write(data []byte) (int, error) {
	if w.file == nil {
		f, err := ioutil.TempFile(os.TempDir(), "")
		Chk.NoError(err)
		w.file = f
		w.writer = io.MultiWriter(f, w.hash)
	}
	return w.writer.Write(data)
}

func (w *fileChunkWriter) Ref() (ref.Ref, error) {
	ref := ref.FromHash(w.hash)
	p := getPath(w.root, ref)
	err := os.MkdirAll(path.Dir(p), 0700)
	Chk.NoError(err)

	err = os.Rename(w.file.Name(), p)
	if err != nil {
		Chk.True(os.IsExist(err))
	}
	return ref, nil
}

func (w *fileChunkWriter) Close() error {
	w.file.Close()
	os.Remove(w.file.Name())
	w.file = nil
	return nil
}

func getPath(root string, ref ref.Ref) string {
	s := ref.String()
	Chk.True(strings.HasPrefix(s, "sha1"))
	return path.Join(root, "sha1", s[5:7], s[7:9], s)
}
