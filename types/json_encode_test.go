package types

import (
	"crypto/sha1"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestJsonEncode(t *testing.T) {
	assert := assert.New(t)
	var s *chunks.MemoryStore

	testEncode := func(expected string, v Value) ref.Ref {
		s = &chunks.MemoryStore{}
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
		assertExists("sha1-dca2a4be23d4455487bb588c6a0ab1b9ee07757e")
	}

	// booleans
	testEncode(`j false
`, Bool(false))
	testEncode(`j true
`, Bool(true))

	// integers
	testEncode(`j {"int16":42}
`, Int16(42))
	testEncode(`j {"int32":0}
`, Int32(0))
	testEncode(`j {"int64":-4611686018427387904}
`, Int64(-1<<62))
	testEncode(`j {"uint16":42}
`, UInt16(42))
	testEncode(`j {"uint32":0}
`, UInt32(0))
	testEncode(`j {"uint64":9223372036854775808}
`, UInt64(1<<63))

	// floats
	testEncode(`j {"float32":88.8}
`, Float32(88.8))
	testEncode(`j {"float64":3.14}
`, Float64(3.14))

	// Strings
	testEncode(`j ""
`, NewString(""))
	testEncode(`j "Hello, World!"
`, NewString("Hello, World!"))

	// Lists
	testEncode(`j {"list":[]}
`, NewList())
	testEncode(`j {"list":["foo",true,{"uint16":42},{"ref":"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea"},{"ref":"sha1-dca2a4be23d4455487bb588c6a0ab1b9ee07757e"}]}
`, NewList(NewString("foo"), Bool(true), UInt16(42), NewList(), NewMap()))
	assertChildVals()

	// Maps
	testEncode(`j {"map":[]}
`, NewMap())
	testEncode(`j {"map":["string","hotdog","list",{"ref":"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea"},"int32",{"int32":42},"bool",false,"map",{"ref":"sha1-dca2a4be23d4455487bb588c6a0ab1b9ee07757e"}]}
`, NewMap(NewString("bool"), Bool(false), NewString("int32"), Int32(42), NewString("string"), NewString("hotdog"), NewString("list"), NewList(), NewString("map"), NewMap()))
	assertChildVals()

	// Sets
	testEncode(`j {"set":[]}
`, NewSet())
}
