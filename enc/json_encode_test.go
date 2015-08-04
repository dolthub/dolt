package enc

import (
	"crypto/sha1"
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
	s := chunks.NopStore{}

	testEncode := func(expected string, v interface{}) ref.Ref {
		w := s.Put()
		err := jsonEncode(v, w)
		assert.NoError(err)

		// Assuming that NopStore works correctly, we don't need to check the actual serialization, only the hash. Neat.
		r, err := w.Ref()
		assert.NoError(err)
		assert.EqualValues(sha1.Sum([]byte(expected)), r.Digest(), "Incorrect ref serializing %+v. Got: %#x", v, r.Digest())
		return r
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

	// Empty compound types
	emptyMapRef := testEncode(`j {"map":[]}
`, Map{})
	emptyListRef := testEncode(`j {"list":[]}
`, []interface{}{})
	testEncode(`j {"set":[]}
`, Set{})

	// Lists
	testEncode(`j {"list":["foo",true,{"uint16":42},{"ref":"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea"},{"ref":"sha1-dca2a4be23d4455487bb588c6a0ab1b9ee07757e"}]}
`, []interface{}{"foo", true, uint16(42), emptyListRef, emptyMapRef})

	// Maps
	testEncode(`j {"map":["string","hotdog","list",{"ref":"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea"},"int32",{"int32":42},"bool",false,"map",{"ref":"sha1-dca2a4be23d4455487bb588c6a0ab1b9ee07757e"}]}
`, MapFromItems("string", "hotdog", "list", emptyListRef, "int32", int32(42), "bool", false, "map", emptyMapRef))

	// Sets
	testEncode(`j {"set":["foo",true,{"uint16":42},{"ref":"sha1-58bdf8e374b39f9b1e8a64784cf5c09601f4b7ea"},{"ref":"sha1-dca2a4be23d4455487bb588c6a0ab1b9ee07757e"}]}
`, Set{"foo", true, uint16(42), emptyListRef, emptyMapRef})

	// Blob (compound)
	blr := ref.MustParse("sha1-5bf524e621975ee2efbf02aed1bc0cd01f1cf8e0")
	cb := CompoundBlob{uint64(2), []uint64{2}, []ref.Ref{blr}}
	testEncode(`j {"cb":[2,2,{"ref":"sha1-5bf524e621975ee2efbf02aed1bc0cd01f1cf8e0"}]}
`, cb)
}
