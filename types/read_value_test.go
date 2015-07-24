package types

import (
	"errors"
	"io"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

type ErrorSource struct {
	chunks.ChunkSource
}

func (e ErrorSource) Get(r ref.Ref) (io.ReadCloser, error) {
	return nil, errors.New("Good golly Miss Molly!")
}

func TestTolerateUngettableRefs(t *testing.T) {
	assert := assert.New(t)
	v, err := ReadValue(ref.Ref{}, &chunks.TestStore{})
	assert.Nil(v)
	assert.NotNil(err)

	v, err = ReadValue(ref.Ref{}, &ErrorSource{})
	assert.Nil(v)
	assert.NotNil(err)
}
