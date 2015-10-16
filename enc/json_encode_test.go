package enc

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/ref"
)

func TestJsonEncode(t *testing.T) {
	assert := assert.New(t)

	// Empty compound types
	ref1 := ref.New(ref.Sha1Digest{0xde, 0xad, 0xbe, 0xef})
	ref2 := ref.New(ref.Sha1Digest{0xbe, 0xef, 0xca, 0xfe})

	testEncode := func(expected string, v interface{}) {
		dst := &bytes.Buffer{}
		jsonEncode(dst, v)
		assert.Equal(expected, string(dst.Bytes()), "Failed to serialize %+v. Got %s instead of %s", v, dst.Bytes(), expected)
	}

	// booleans
	testEncode(`j false
`, false)
	testEncode(`j true
`, true)

	// integers
	testEncode(`j {"int8":42}
`, int8(42))
	testEncode(`j {"int16":42}
`, int16(42))
	testEncode(`j {"int32":0}
`, int32(0))
	testEncode(`j {"int64":-4611686018427387904}
`, int64(-1<<62))
	testEncode(`j {"uint8":42}
`, uint8(42))
	testEncode(`j {"uint16":42}
`, uint16(42))
	testEncode(`j {"uint32":0}
`, uint32(0))
	testEncode(`j {"uint64":9223372036854775808}
`, uint64(1<<63))

	// floats
	testEncode(`j {"float32":88.8}
`, float32(88.8))
	testEncode(`j {"float64":3.14}
`, float64(3.14))

	// Strings
	testEncode(`j ""
`, "")
	testEncode(`j "Hello, World!"
`, "Hello, World!")

	// Lists
	testEncode(`j {"list":[]}
`, []interface{}{})
	expected := fmt.Sprintf(`j {"list":["foo",true,{"uint16":42},{"ref":"%s"},{"ref":"%s"}]}
`, ref2, ref1)
	testEncode(expected, []interface{}{"foo", true, uint16(42), ref2, ref1})

	// Maps
	testEncode(`j {"map":[]}
`, Map{})
	expected = fmt.Sprintf(`j {"map":["string","hotdog","list",{"ref":"%s"},"int32",{"int32":42},"bool",false,"map",{"ref":"%s"}]}
`, ref2, ref1)
	testEncode(expected, MapFromItems("string", "hotdog", "list", ref2, "int32", int32(42), "bool", false, "map", ref1))

	// Sets
	testEncode(`j {"set":[]}
`, Set{})
	expected = fmt.Sprintf(`j {"set":["foo",true,{"uint16":42},{"ref":"%s"},{"ref":"%s"}]}
`, ref2, ref1)
	testEncode(expected, SetFromItems("foo", true, uint16(42), ref2, ref1))

	// TypeRefs
	expected = `j {"type":{"kind":{"uint8":0},"name":""}}
`
	testEncode(expected, TypeRef{ref.Ref{}, "", 0, nil})
	expected = fmt.Sprintf(`j {"type":{"desc":{"list":[{"ref":"%s"},{"ref":"%s"}]},"kind":{"uint8":15},"name":""}}
`, ref1, ref2)
	testEncode(expected, TypeRef{ref.Ref{}, "", 15, []interface{}{ref1, ref2}})
	expected = `j {"type":{"desc":{"list":["f","g"]},"kind":{"uint8":18},"name":"enum"}}
`
	testEncode(expected, TypeRef{ref.Ref{}, "enum", 18, []interface{}{"f", "g"}})

	pkgRef := ref1
	expected = fmt.Sprintf(`j {"type":{"desc":{"int16":42},"kind":{"uint8":20},"name":"","pkgRef":{"ref":"%s"}}}
`, pkgRef)
	testEncode(expected, TypeRef{Kind: 20, Name: "", PkgRef: pkgRef, Desc: int16(42)})

	// Blob (compound)
	testEncode(fmt.Sprintf(`j {"cb":["%s",2]}
`, ref2), CompoundBlob{[]uint64{2}, []ref.Ref{ref2}})

	// List (compound)
	testEncode(fmt.Sprintf(`j {"cl":["%s",2]}
`, ref2), CompoundList{[]uint64{2}, []ref.Ref{ref2}})
}
