package chunks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPut(t *testing.T) {
	assert := assert.New(t)
	s := NopStore{}

	input := "abc"
	w := s.Put()
	_, err := w.Write([]byte(input))
	assert.NoError(err)
	ref := w.Ref()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	assert.Equal("sha1-a9993e364706816aba3e25717850c26c9cd0d89d", ref.String())
}
