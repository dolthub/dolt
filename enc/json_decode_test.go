package enc

import (
	"fmt"
	"strings"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

func TestJSONDecode(t *testing.T) {
	assert := assert.New(t)

	put := func(s string) ref.Ref {
		s += "\n"
		c := chunks.NewChunk([]byte(s))
		return c.Ref()
	}

	emptyListRef := put(`j {"list":[]}`)
	emptyMapRef := put(`j {"map":[]}`)

	testDecode := func(s string, expected interface{}) {
		actual := jsonDecode(strings.NewReader(s))
		assert.EqualValues(expected, actual, "Expected decoded value: %s to equal: %+v, but was: %+v", s, expected, actual)
	}

	// integers
	testDecode(`j {"int8":42}
`, int8(42))
	testDecode(`j {"int16":42}
`, int16(42))
	testDecode(`j {"int32":0}
`, int32(0))
	testDecode(`j {"int64":-4611686018427387904}
`, int64(-1<<62))
	testDecode(`j {"uint8":42}
`, uint8(42))
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

	// TypeRefs
	testDecode(`j {"type":{"kind":{"uint8":0},"name":""}}
`, TypeRef{Kind: 0})
	ref1 := ref.New(ref.Sha1Digest{0xde, 0xad, 0xbe, 0xef})
	ref2 := ref.New(ref.Sha1Digest{0xbe, 0xef, 0xca, 0xfe})
	testDecode(fmt.Sprintf(`j {"type":{"desc":{"list":[{"ref":"%s"},{"ref":"%s"}]},"kind":{"uint8":15},"name":""}}
`, ref1, ref2), TypeRef{"", 15, []interface{}{ref1, ref2}})
	testDecode(`j {"type":{"desc":{"list":["f","g"]},"kind":{"uint8":18},"name":"enum"}}
`, TypeRef{"enum", 18, []interface{}{"f", "g"}})

	pkgRef := ref1
	testDecode(fmt.Sprintf(`j {"type":{"name":"","kind":{"uint8":20},"desc":{"list":[{"ref":"%s"},{"int16":42}]}}}
`, pkgRef), TypeRef{Kind: 20, Name: "", Desc: []interface{}{pkgRef, int16(42)}})

	// Blob (compound)
	// echo -n 'b Hello' | sha1sum
	blr := ref.Parse("sha1-c35018551e725bd2ab45166b69d15fda00b161c1")
	cb := CompoundBlob{[]uint64{2}, []ref.Ref{blr}}
	testDecode(`j {"cb":["sha1-c35018551e725bd2ab45166b69d15fda00b161c1",2]}
`, cb)
	// echo -n 'b  ' | sha1sum
	blr2 := ref.Parse("sha1-641283a12b475ed58ba510517c1224a912e934a6")
	// echo -n 'b World!' | sha1sum
	blr3 := ref.Parse("sha1-8169c017ce2779f3f66bfe27ee2313d71f7698b9")
	cb2 := CompoundBlob{[]uint64{5, 6, 12}, []ref.Ref{blr, blr2, blr3}}
	testDecode(`j {"cb":["sha1-c35018551e725bd2ab45166b69d15fda00b161c1",5,"sha1-641283a12b475ed58ba510517c1224a912e934a6",1,"sha1-8169c017ce2779f3f66bfe27ee2313d71f7698b9",6]}
`, cb2)

	// List (compound)
	// echo -n 'b Hello' | sha1sum
	llr := ref.Parse("sha1-c35018551e725bd2ab45166b69d15fda00b161c1")
	cl := CompoundList{[]uint64{2}, []ref.Ref{llr}}
	testDecode(`j {"cl":["sha1-c35018551e725bd2ab45166b69d15fda00b161c1",2]}
`, cl)
	// echo -n 'b  ' | sha1sum
	llr2 := ref.Parse("sha1-641283a12b475ed58ba510517c1224a912e934a6")
	// echo -n 'b World!' | sha1sum
	llr3 := ref.Parse("sha1-8169c017ce2779f3f66bfe27ee2313d71f7698b9")
	cl2 := CompoundList{[]uint64{5, 6, 12}, []ref.Ref{llr, llr2, llr3}}
	testDecode(`j {"cl":["sha1-c35018551e725bd2ab45166b69d15fda00b161c1",5,"sha1-641283a12b475ed58ba510517c1224a912e934a6",1,"sha1-8169c017ce2779f3f66bfe27ee2313d71f7698b9",6]}
`, cl2)

	// Package
	testDecode(`j {"package":{"dependencies":[],"types":[]}}
`, Package{Types: []TypeRef{}, Dependencies: []ref.Ref{}})
	testDecode(`j {"package":{"dependencies":[],"types":[{"type":{"kind":{"uint8":0},"name":""}}]}}
`, Package{Types: []TypeRef{TypeRef{Kind: 0, Name: ""}}, Dependencies: []ref.Ref{}})
	testDecode(fmt.Sprintf(`j {"package":{"dependencies":[{"ref":"%s"}],"types":[]}}
`, ref1), Package{Types: []TypeRef{}, Dependencies: []ref.Ref{ref1}})
}

func TestCompoundBlobJSONDecodeInvalidFormat(t *testing.T) {
	assert := assert.New(t)

	d.IsUsageError(assert, func() {
		jsonDecode(strings.NewReader("j {\"cb\":[]}\n"))
	})

	d.IsUsageError(assert, func() {
		jsonDecode(strings.NewReader("j {\"cb\":[2]}\n"))
	})

	d.IsUsageError(assert, func() {
		jsonDecode(strings.NewReader("j {\"cb\":[true]}\n"))
	})

	d.IsUsageError(assert, func() {
		jsonDecode(strings.NewReader("j {\"cb\":[\"hi\"]}\n"))
	})

	d.IsUsageError(assert, func() {
		jsonDecode(strings.NewReader(`j {"cb":["sha1-c35018551e725bd2ab45166b69d15fda00b161c1",2.5]}
`))
	})

	d.IsUsageError(assert, func() {
		jsonDecode(strings.NewReader(`j {"cb":["sha1-c35018551e725bd2ab45166b69d15fda00b161c1"]}
`))
	})

	d.IsUsageError(assert, func() {
		jsonDecode(strings.NewReader(`j {"cb":["invalid ref",2]}
`))
	})
}
