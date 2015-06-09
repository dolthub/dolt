package store

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileStorePut(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir(os.TempDir(), "")
	defer os.Remove(dir)
	assert.NoError(err)

	input := "abc"
	s := NewFileStore(dir)
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
	reader, err := s.Get(ref)
	assert.NoError(err)
	data, err = ioutil.ReadAll(reader)
	assert.NoError(err)
	assert.Equal(input, string(data))
}
