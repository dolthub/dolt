package enc

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/store"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestStringCodec(t *testing.T) {
	assert := assert.New(t)

	dir, err := ioutil.TempDir(os.TempDir(), "")
	defer os.Remove(dir)
	assert.NoError(err)
	fs := store.NewFileStore(dir)
	s1 := types.NewString("")
	r1, err := encodeString(s1, fs)
	// echo -n 'b ' | sha1sum
	assert.Equal("sha1-d42929a8e629463cc7d3c859bdc8e25db35d6963", r1.String())

	s2 := types.NewString("Hello, World!")
	r2, err := encodeString(s2, fs)
	// echo -n 's Hello, World!' | sha1sum
	assert.Equal("sha1-7a4bf80c1684eadbc686d4b970beddd330c48073", r2.String())

	reader, err := fs.Get(r1)
	assert.NoError(err)
	v1, err := decodeString(reader, fs)
	assert.NoError(err)
	assert.True(s1.Equals(v1))

	reader, err = fs.Get(r2)
	assert.NoError(err)
	v2, err := decodeString(reader, fs)
	assert.NoError(err)
	assert.True(s2.Equals(v2))
}
