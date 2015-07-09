package types

import (
	"strings"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/stretchr/testify/assert"
)

func TestJSONDecode(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.MemoryStore{}

	put := func(s string) {
		s += "\n"
		w := cs.Put()
		_, err := w.Write([]byte(s))
		assert.NoError(err)
		r, err := w.Ref()
		assert.NotNil(r)
		assert.NoError(err)
	}

	put(`j {"list":[]}`)
	put(`j {"map":[]}`)

	testDecode := func(s string, expected Value) {
		actual, err := jsonDecode(strings.NewReader(s), &cs)
		assert.NoError(err)
		assert.True(expected.Equals(actual), "Expected decoded value: %s to equal: %+v, but was: %+v", s, expected, actual)
	}

	// integers
	testDecode(`j {"int16":42}
`, Int16(42))
	testDecode(`j {"int32":0}
`, Int32(0))
	testDecode(`j {"int64":-4611686018427387904}
`, Int64(-1<<62))
	testDecode(`j {"uint16":42}
`, UInt16(42))
	testDecode(`j {"uint32":0}
`, UInt32(0))
	testDecode(`j {"uint64":9223372036854775808}
`, UInt64(1<<63))

	// floats
	testDecode(`j {"float32":88.8}
`, Float32(88.8))
	testDecode(`j {"float64":3.14}
`, Float64(3.14))

	// Strings
	testDecode(`j ""
`, NewString(""))
	testDecode(`j "Hello, World!"
`, NewString("Hello, World!"))

	// Lists
	testDecode(`j {"list":[]}
`, NewList())
	testDecode(`j {"list":["foo",true,{"uint16":42},{"ref":"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea"},{"ref":"sha1-dca2a4be23d4455487bb588c6a0ab1b9ee07757e"}]}
	//`, NewList(NewString("foo"), Bool(true), UInt16(42), NewList(), NewMap()))

	// Maps
	testDecode(`j {"map":[]}
`, NewMap())
	testDecode(`j {"map":["string","hotdog","list",{"ref":"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea"},"int32",{"int32":42},"bool",false,"map",{"ref":"sha1-dca2a4be23d4455487bb588c6a0ab1b9ee07757e"}]}
	//`, NewMap(NewString("bool"), Bool(false), NewString("int32"), Int32(42), NewString("string"), NewString("hotdog"), NewString("list"), NewList(), NewString("map"), NewMap()))

	// Sets
	testDecode(`j {"set":[]}
`, NewSet())
	testDecode(`j {"set":[{"int32":42},"hotdog",{"ref":"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea"},false,{"ref":"sha1-dca2a4be23d4455487bb588c6a0ab1b9ee07757e"}]}
`, NewSet(Bool(false), Int32(42), NewString("hotdog"), NewList(), NewMap()))

	// referenced blobs?
}
