package enc

import (
	"strings"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestJSONDecode(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NopStore{}

	put := func(s string) ref.Ref {
		s += "\n"
		w := cs.Put()
		_, err := w.Write([]byte(s))
		assert.NoError(err)
		r, err := w.Ref()
		assert.NotNil(r)
		assert.NoError(err)
		return r
	}

	emptyListRef := put(`j {"list":[]}`)
	emptyMapRef := put(`j {"map":[]}`)

	testDecode := func(s string, expected interface{}) {
		actual, err := jsonDecode(strings.NewReader(s))
		assert.NoError(err)
		assert.EqualValues(expected, actual, "Expected decoded value: %s to equal: %+v, but was: %+v", s, expected, actual)
	}

	// integers
	testDecode(`j {"int16":42}
`, int16(42))
	testDecode(`j {"int32":0}
`, int32(0))
	testDecode(`j {"int64":-4611686018427387904}
`, int64(-1<<62))
	testDecode(`j {"uint16":42}
`, uint16(42))
	testDecode(`j {"uint32":0}
`, uint32(0))
	testDecode(`j {"uint64":9223372036854775808}
`, uint64(1<<63))

	// floats
	testDecode(`j {"float32":88.8}
`, float32(88.8))
	testDecode(`j {"float64":3.14}
`, float64(3.14))

	// Strings
	testDecode(`j ""
`, "")
	testDecode(`j "Hello, World!"
`, "Hello, World!")

	// Lists
	testDecode(`j {"list":[]}
`, []interface{}{})
	testDecode(`j {"list":["foo",true,{"uint16":42},{"ref":"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea"},{"ref":"sha1-dca2a4be23d4455487bb588c6a0ab1b9ee07757e"}]}
`, []interface{}{"foo", true, uint16(42), emptyListRef, emptyMapRef})

	// Maps
	testDecode(`j {"map":[]}
`, Map{})
	testDecode(`j {"map":["string","hotdog","list",{"ref":"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea"},"int32",{"int32":42},"bool",false,"map",{"ref":"sha1-dca2a4be23d4455487bb588c6a0ab1b9ee07757e"}]}
`, MapFromItems("string", "hotdog", "list", emptyListRef, "int32", int32(42), "bool", false, "map", emptyMapRef))

	// Sets
	testDecode(`j {"set":[]}
`, Set{})
	testDecode(`j {"set":[false,{"int32":42},"hotdog",{"ref":"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea"},{"ref":"sha1-dca2a4be23d4455487bb588c6a0ab1b9ee07757e"}]}
`, SetFromItems(false, int32(42), "hotdog", emptyListRef, emptyMapRef))

	// Blob (compound)
	// echo -n 'b Hello' | sha1sum
	blr := ref.MustParse("sha1-c35018551e725bd2ab45166b69d15fda00b161c1")
	cb := CompoundBlob{uint64(2), []uint64{0}, []ref.Ref{blr}}
	testDecode(`j {"cb":[{"ref":"sha1-c35018551e725bd2ab45166b69d15fda00b161c1"},2]}
`, cb)
	// echo -n 'b  ' | sha1sum
	blr2 := ref.MustParse("sha1-641283a12b475ed58ba510517c1224a912e934a6")
	// echo -n 'b World!' | sha1sum
	blr3 := ref.MustParse("sha1-8169c017ce2779f3f66bfe27ee2313d71f7698b9")
	cb2 := CompoundBlob{uint64(12), []uint64{0, 5, 6}, []ref.Ref{blr, blr2, blr3}}
	testDecode(`j {"cb":[{"ref":"sha1-c35018551e725bd2ab45166b69d15fda00b161c1"},5,{"ref":"sha1-641283a12b475ed58ba510517c1224a912e934a6"},6,{"ref":"sha1-8169c017ce2779f3f66bfe27ee2313d71f7698b9"},12]}
`, cb2)
}

func TestCompoundBlobJSONDecodeInvalidFormat(t *testing.T) {
	assert := assert.New(t)

	_, err := jsonDecode(strings.NewReader("j {\"cb\":[]}\n"))
	assert.Error(err)
	_, err = jsonDecode(strings.NewReader("j {\"cb\":[2]}\n"))
	assert.Error(err)

	_, err = jsonDecode(strings.NewReader("j {\"cb\":[true]}\n"))
	assert.Error(err)
	_, err = jsonDecode(strings.NewReader("j {\"cb\":[\"hi\"]}\n"))
	assert.Error(err)

	_, err = jsonDecode(strings.NewReader(`j {"cb":[{"ref":"sha1-c35018551e725bd2ab45166b69d15fda00b161c1"},2.5]}
`))
	assert.Error(err)

	_, err = jsonDecode(strings.NewReader(`j {"cb":[{"ref":"sha1-c35018551e725bd2ab45166b69d15fda00b161c1"}]}
`))
	assert.Error(err)

	_, err = jsonDecode(strings.NewReader(`j {"cb":[{"ref":"invalid ref"},2]}
`))
	assert.Error(err)
}
