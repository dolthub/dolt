package enc

import (
	"bytes"
	"crypto/sha1"
	"io/ioutil"
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

	assertExists := func(refStr, expected string) {
		ref := ref.MustParse(refStr)
		r, err := s.Get(ref)
		assert.NoError(err)
		b, err := ioutil.ReadAll(r)
		assert.NoError(err)
		assert.True(bytes.Equal([]byte(expected), b), "Expected ref %s to be %s", refStr, expected)
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

	// Blobs we delegate to the blob codec. That's tested in blob_codec_test.go, but let's just make sure the delegation is working.
	testEncode(string([]byte{'b', ' ', 0x00, 0x01, 0x02}), types.NewBlob([]byte{0x00, 0x01, 0x02}))

	// Strings
	testEncode("j \"\"\n", types.NewString(""))
	testEncode("j \"Hello, World!\"\n", types.NewString("Hello, World!"))

	// Lists
	testEncode("j {\"list\":[]}\n", types.NewList())
	testEncode("j {\"list\":[\"foo\",true,{\"uint16\":42},{\"ref\":\"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea\"},{\"ref\":\"sha1-fa8026bf44f60b64ab674c49cda31a697467973c\"}]}\n",
		types.NewList(types.NewString("foo"), types.Bool(true), types.UInt16(42), types.NewList(), types.NewMap()))
	assertChildVals := func() {
		assert.Equal(3, s.Len())
		assertExists("sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea", "j {\"list\":[]}\n")
		assertExists("sha1-fa8026bf44f60b64ab674c49cda31a697467973c", "j {\"map\":{}}\n")
	}
	assertChildVals()

	// Maps
	testEncode("j {\"map\":{}}\n", types.NewMap())
	testEncode("j {\"map\":{\"bool\":false,\"int32\":{\"int32\":42},\"list\":{\"ref\":\"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea\"},\"map\":{\"ref\":\"sha1-fa8026bf44f60b64ab674c49cda31a697467973c\"},\"string\":\"hotdog\"}}\n",
		types.NewMap("bool", types.Bool(false), "int32", types.Int32(42), "string", types.NewString("hotdog"), "list", types.NewList(), "map", types.NewMap()))
	assertChildVals()
}
