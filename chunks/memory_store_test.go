package chunks

import (
	"io/ioutil"
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestMemoryStorePut(t *testing.T) {
	assert := assert.New(t)

	s := MemoryStore{}
	assert.Equal(0, s.Len())

	input := "abc"
	w := s.Put()
	_, err := w.Write([]byte(input))
	assert.NoError(err)
	ref, err := w.Ref()
	assert.NoError(err)

	// See http://www.di-mgt.com.au/sha_testvectors.html
	assert.Equal("sha1-a9993e364706816aba3e25717850c26c9cd0d89d", ref.String())

	// Reading it back via the API should work...
	reader, err := s.Get(ref)
	assert.NoError(err)
	data, err := ioutil.ReadAll(reader)
	assert.NoError(err)
	assert.Equal(input, string(data))

	assert.Equal(1, s.Len())
}
