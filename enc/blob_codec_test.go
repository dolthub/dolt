package enc

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/store"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestBlobCodec(t *testing.T) {
	assert := assert.New(t)

	dir, err := ioutil.TempDir(os.TempDir(), "")
	defer os.Remove(dir)
	assert.NoError(err)
	fs := store.NewFileStore(dir, "root")
	b1 := types.NewBlob([]byte{})
	r1, err := blobEncode(b1, fs)
	// echo -n 'b ' | sha1sum
	assert.Equal("sha1-e1bc846440ec2fb557a5a271e785cd4c648883fa", r1.String())

	b2 := types.NewBlob([]byte("Hello, World!"))
	r2, err := blobEncode(b2, fs)
	// echo -n 'b Hello, World!' | sha1sum
	assert.Equal("sha1-135fe1453330547994b2ce8a1b238adfbd7df87e", r2.String())

	reader, err := fs.Get(r1)
	assert.NoError(err)
	v1, err := blobDecode(reader, fs)
	assert.NoError(err)
	assert.True(b1.Equals(v1))

	reader, err = fs.Get(r2)
	assert.NoError(err)
	v2, err := blobDecode(reader, fs)
	assert.NoError(err)
	assert.True(b2.Equals(v2))
}
