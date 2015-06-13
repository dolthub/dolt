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

	assertExists := func(refStr string) {
		ref := ref.MustParse(refStr)
		r, err := s.Get(ref)
		defer r.Close()
		assert.NoError(err)
		assert.NotNil(r)
	}

	assertChildVals := func() {
		assert.Equal(3, s.Len())
		assertExists("sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea")
		assertExists("sha1-fa8026bf44f60b64ab674c49cda31a697467973c")
	}

	// booleans
	testEncode(`j false
`, types.Bool(false))
	testEncode(`j true
`, types.Bool(true))

	// integers
	testEncode(`j {"int16":42}
`, types.Int16(42))
	testEncode(`j {"int32":0}
`, types.Int32(0))
	testEncode(`j {"int64":-4611686018427387904}
`, types.Int64(-1<<62))
	testEncode(`j {"uint16":42}
`, types.UInt16(42))
	testEncode(`j {"uint32":0}
`, types.UInt32(0))
	testEncode(`j {"uint64":9223372036854775808}
`, types.UInt64(1<<63))

	// floats
	testEncode(`j {"float32":88.8}
`, types.Float32(88.8))
	testEncode(`j {"float64":3.14}
`, types.Float64(3.14))

	// Strings
	testEncode(`j ""
`, types.NewString(""))
	testEncode(`j "Hello, World!"
`, types.NewString("Hello, World!"))

	// Lists
	testEncode(`j {"list":[]}
`, types.NewList())
	testEncode(`j {"list":["foo",true,{"uint16":42},{"ref":"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea"},{"ref":"sha1-fa8026bf44f60b64ab674c49cda31a697467973c"}]}
`, types.NewList(types.NewString("foo"), types.Bool(true), types.UInt16(42), types.NewList(), types.NewMap()))
	assertChildVals()

	// Maps
	testEncode(`j {"map":{}}
`, types.NewMap())
	testEncode(`j {"map":{"bool":false,"int32":{"int32":42},"list":{"ref":"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea"},"map":{"ref":"sha1-fa8026bf44f60b64ab674c49cda31a697467973c"},"string":"hotdog"}}
`, types.NewMap("bool", types.Bool(false), "int32", types.Int32(42), "string", types.NewString("hotdog"), "list", types.NewList(), "map", types.NewMap()))
	assertChildVals()

	// Sets
	testEncode(`j {"set":[]}
`, types.NewSet())
}
