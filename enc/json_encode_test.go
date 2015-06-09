package enc

import (
	"crypto/sha1"
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/store"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestJsonEncode(t *testing.T) {
	assert := assert.New(t)
	var s *store.MemoryStore

	testEncode := func(expected string, v types.Value) ref.Ref {
		s = &store.MemoryStore{}
		r, err := jsonEncode(v, s)
		assert.NoError(err)

		// Assuming that MemoryStore works correctly, we don't need to check the actual serialization, only the hash. Neat.
		assert.EqualValues(sha1.Sum([]byte(expected)), r.Digest(), "Incorrect ref serializing %+v. Got: %#x", v, r.Digest())
		return r
	}

	// booleans
	testEncode("j false\n", types.Bool(false))
	testEncode("j true\n", types.Bool(true))

	// integers
	testEncode("j {\"int16\":42}\n", types.Int16(42))
	testEncode("j {\"int32\":0}\n", types.Int32(0))
	testEncode("j {\"int64\":-4611686018427387904}\n", types.Int64(-1<<62))
	testEncode("j {\"uint16\":42}\n", types.UInt16(42))
	testEncode("j {\"uint32\":0}\n", types.UInt32(0))
	testEncode("j {\"uint64\":9223372036854775808}\n", types.UInt64(1<<63))

	// blobs we delegate to the blob codec. That's tested in blob_codec_test.go, but let's just make sure the delegation is working.
	testEncode(string([]byte{'b', ' ', 0x00, 0x01, 0x02}), types.NewBlob([]byte{0x00, 0x01, 0x02}))

	// strings - todo
	//testEncode("j 'Hello, World!'", types.NewString("Hello, World!"))
}
