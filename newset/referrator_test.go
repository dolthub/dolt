package newset

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
)

func TestReferrator(t *testing.T) {
	assert := assert.New(t)

	ator := newReferrator()
	assert.Equal("sha1-0000000000000000000000000000000000000000", ator.Next().String())
	assert.Equal("sha1-0000000000000000000000000000000000000001", ator.Next().String())
	for i := 0; i < 510; i++ {
		ator.Next()
	}
	assert.Equal("sha1-0000000000000000000000000000000000000200", ator.Next().String())
}
