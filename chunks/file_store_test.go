package chunks

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestFileStorePut(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir(os.TempDir(), "")
	defer os.Remove(dir)
	assert.NoError(err)

	input := "abc"
	s := NewFileStore(dir, "root")
	w := s.Put()
	_, err = w.Write([]byte(input))
	assert.NoError(err)
	ref, err := w.Ref()
	assert.NoError(err)

	// See http://www.di-mgt.com.au/sha_testvectors.html
	assert.Equal("sha1-a9993e364706816aba3e25717850c26c9cd0d89d", ref.String())

	// There should also be a file there now...
	p := path.Join(dir, "sha1", "a9", "99", ref.String())
	f, err := os.Open(p)
	assert.NoError(err)
	data, err := ioutil.ReadAll(f)
	assert.NoError(err)
	assert.Equal(input, string(data))

	// And reading it via the API should work...
	assertInputInStore(input, ref, s, assert)
}

func TestFileStorePutWithRefAfterClose(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir(os.TempDir(), "")
	defer os.Remove(dir)
	assert.NoError(err)

	input := "abc"
	s := NewFileStore(dir, "root")
	w := s.Put()
	_, err = w.Write([]byte(input))
	assert.NoError(err)

	assert.NoError(w.Close())
	ref, err := w.Ref() // Ref() after Close() should work...
	assert.NoError(err)

	// And reading the data via the API should work...
	assertInputInStore(input, ref, &s, assert)
}

func TestFileStorePutWithMultipleRef(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir(os.TempDir(), "")
	defer os.Remove(dir)
	assert.NoError(err)

	input := "abc"
	s := NewFileStore(dir, "root")
	w := s.Put()
	_, err = w.Write([]byte(input))
	assert.NoError(err)

	_, _ = w.Ref()
	assert.NoError(err)
	ref, err := w.Ref() // Multiple calls to Ref() should work...
	assert.NoError(err)

	// And reading the data via the API should work...
	assertInputInStore(input, ref, &s, assert)
}

func TestFileStoreRoot(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir(os.TempDir(), "")
	defer os.Remove(dir)
	assert.NoError(err)

	s := NewFileStore(dir, "root")
	oldRoot := s.Root()
	assert.Equal(oldRoot, ref.Ref{})

	// Root file should be absent
	f, err := os.Open(path.Join(dir, "root"))
	assert.True(os.IsNotExist(err))

	bogusRoot, err := ref.Parse("sha1-81c870618113ba29b6f2b396ea3a69c6f1d626c5") // sha1("Bogus, Dude")
	assert.NoError(err)
	newRoot, err := ref.Parse("sha1-907d14fb3af2b0d4f18c2d46abe8aedce17367bd") // sha1("Hello, World")
	assert.NoError(err)

	// Try to update root with bogus oldRoot
	result := s.UpdateRoot(newRoot, bogusRoot)
	assert.False(result)

	// Root file should now be there, but should be empty
	f, err = os.Open(path.Join(dir, "root"))
	assert.NoError(err)
	input, err := ioutil.ReadAll(f)
	assert.Equal(len(input), 0)

	// Now do a valid root update
	result = s.UpdateRoot(newRoot, oldRoot)
	assert.True(result)

	// Root file should now contain "Hello, World" sha1
	f, err = os.Open(path.Join(dir, "root"))
	assert.NoError(err)
	input, err = ioutil.ReadAll(f)
	assert.NoError(err)
	assert.Equal("sha1-907d14fb3af2b0d4f18c2d46abe8aedce17367bd", string(input))
}
