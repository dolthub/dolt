package chunks

import (
	"io/ioutil"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func assertInputInStore(input string, ref ref.Ref, s ChunkStore, assert *assert.Assertions) {
	reader, err := s.Get(ref)
	assert.NoError(err)
	data, err := ioutil.ReadAll(reader)
	assert.NoError(err)
	assert.Equal(input, string(data))
}
