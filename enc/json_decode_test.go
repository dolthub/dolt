package enc

import (
	"strings"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/types"
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

	testDecode := func(s string, expected types.Value) {
		actual, err := jsonDecode(strings.NewReader(s), &cs)
		assert.NoError(err)
		assert.True(expected.Equals(actual), "Expected decoded value: %s to equal: %+v, but was: %+v", s, expected, actual)
	}

	// integers
	testDecode(`j {"int16":42}
`, types.Int16(42))
	testDecode(`j {"int32":0}
`, types.Int32(0))
	testDecode(`j {"int64":-4611686018427387904}
`, types.Int64(-1<<62))
	testDecode(`j {"uint16":42}
`, types.UInt16(42))
	testDecode(`j {"uint32":0}
`, types.UInt32(0))
	testDecode(`j {"uint64":9223372036854775808}
`, types.UInt64(1<<63))

	// floats
	testDecode(`j {"float32":88.8}
`, types.Float32(88.8))
	testDecode(`j {"float64":3.14}
`, types.Float64(3.14))

	// Strings
	testDecode(`j ""
`, types.NewString(""))
	testDecode(`j "Hello, World!"
`, types.NewString("Hello, World!"))

	// Lists
	testDecode(`j {"list":[]}
`, types.NewList())
	testDecode(`j {"list":["foo",true,{"uint16":42},{"ref":"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea"},{"ref":"sha1-dca2a4be23d4455487bb588c6a0ab1b9ee07757e"}]}
	//`, types.NewList(types.NewString("foo"), types.Bool(true), types.UInt16(42), types.NewList(), types.NewMap()))

	// Maps
	testDecode(`j {"map":[]}
`, types.NewMap())
	testDecode(`j {"map":["string","hotdog","list",{"ref":"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea"},"int32",{"int32":42},"bool",false,"map",{"ref":"sha1-dca2a4be23d4455487bb588c6a0ab1b9ee07757e"}]}
	//`, types.NewMap(types.NewString("bool"), types.Bool(false), types.NewString("int32"), types.Int32(42), types.NewString("string"), types.NewString("hotdog"), types.NewString("list"), types.NewList(), types.NewString("map"), types.NewMap()))

	// Sets
	testDecode(`j {"set":[]}
`, types.NewSet())
	testDecode(`j {"set":[{"int32":42},"hotdog",{"ref":"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea"},false,{"ref":"sha1-dca2a4be23d4455487bb588c6a0ab1b9ee07757e"}]}
`, types.NewSet(types.Bool(false), types.Int32(42), types.NewString("hotdog"), types.NewList(), types.NewMap()))

	// referenced blobs?
}
