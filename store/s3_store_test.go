package store

import (
	"crypto/sha1"
	"io/ioutil"
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestS3StorePut(t *testing.T) {
	assert := assert.New(t)

	input := "abc"
	s := NewS3Store()

	w := s.Put()
	_, err := w.Write([]byte(input))
	assert.NoError(err)

	r1, err := w.Ref()
	assert.NoError(err)

	// See http://www.di-mgt.com.au/sha_testvectors.html
	assert.Equal("sha1-a9993e364706816aba3e25717850c26c9cd0d89d", r1.String())

	// And reading it via the API should work...
	reader, err := s.Get(r1)
	assert.NoError(err)

	data, err := ioutil.ReadAll(reader)
	assert.NoError(err)
	assert.Equal(input, string(data))

	// Reading a non-existing ref fails
	digest := ref.Sha1Digest{}
	hash := sha1.New()
	hash.Write([]byte("Non-existent"))
	hash.Sum(digest[:0])
	r2 := ref.New(digest)
	reader, err = s.Get(r2)
	assert.Error(err)
}
