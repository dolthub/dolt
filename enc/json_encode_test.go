package enc

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

type logChunkWriter struct {
	chunks.ChunkWriter
}

func (w *logChunkWriter) Write(data []byte) (int, error) {
	fmt.Println(data)
	return chunks.ChunkWriter.Write(w, data)
}

func TestJsonEncode(t *testing.T) {
	assert := assert.New(t)

	// Empty compound types
	ref1 := ref.New(ref.Sha1Digest{0xde, 0xad, 0xbe, 0xef})
	ref2 := ref.New(ref.Sha1Digest{0xbe, 0xef, 0xca, 0xfe})

	testEncode := func(expected string, v interface{}) {
		dst := &bytes.Buffer{}
		assert.NoError(jsonEncode(dst, v))
		assert.Equal(expected, string(dst.Bytes()), "Failed to serialize %+v. Got %s instead of %s", v, dst.Bytes(), expected)
	}

	// booleans
	testEncode(`j false
`, false)
	testEncode(`j true
`, true)

	// integers
	testEncode(`j {"int16":42}
`, int16(42))
	testEncode(`j {"int32":0}
`, int32(0))
	testEncode(`j {"int64":-4611686018427387904}
`, int64(-1<<62))
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
`, encMap{})
	expected = fmt.Sprintf(`j {"map":["string","hotdog","list",{"ref":"%s"},"int32",{"int32":42},"bool",false,"map",{"ref":"%s"}]}
`, ref2, ref1)
	testEncode(expected, MapFromItems("string", "hotdog", "list", ref2, "int32", int32(42), "bool", false, "map", ref1))

	// Sets
	testEncode(`j {"set":[]}
`, set{})
	expected = fmt.Sprintf(`j {"set":["foo",true,{"uint16":42},{"ref":"%s"},{"ref":"%s"}]}
`, ref2, ref1)
	testEncode(expected, set{"foo", true, uint16(42), ref2, ref1})

	// Blob (compound)
	testEncode(fmt.Sprintf(`j {"cb":[{"ref":"%s"},2]}
`, ref2), compoundBlob{uint64(2), []uint64{0}, []ref.Ref{ref2}})
}
