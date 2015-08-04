package types

import (
	"crypto/sha1"
	"os"
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

	// Blob (compound)
	blr := ref.MustParse("sha1-5bf524e621975ee2efbf02aed1bc0cd01f1cf8e0")
	cb := compoundBlob{uint64(2), []uint64{0}, []Future{futureFromRef(blr)}, &ref.Ref{}, s}
	testEncode(`j {"cb":[{"ref":"sha1-5bf524e621975ee2efbf02aed1bc0cd01f1cf8e0"},2]}
`, cb)

	bl := newBlobLeaf([]byte("hello"))
	cb = compoundBlob{uint64(5), []uint64{0}, []Future{futureFromValue(bl)}, &ref.Ref{}, s}
	testEncode(`j {"cb":[{"ref":"sha1-8543a1b775237567a8c0e70e8ae7a1c6aac0ebbb"},5]}
`, cb)
}

func TestGetJSONChildResolvedFuture(t *testing.T) {
	assert := assert.New(t)
	cs := &chunks.TestStore{}
	v := NewString("abc")
	f := futureFromValue(v)
	o, err := getChildJSON(f, cs)
	assert.NoError(err)
	assert.Equal("abc", o)
	assert.Equal(0, cs.Reads)
}

func TestGetJSONChildUnresolvedFuture(t *testing.T) {
	assert := assert.New(t)
	cs := &chunks.TestStore{}
	s := "sha1-a9993e364706816aba3e25717850c26c9cd0d89d"
	r := ref.MustParse(s)
	f := futureFromRef(r)
	m, err := getChildJSON(f, cs)
	assert.NoError(err)
	assert.Equal(s, m.(map[string]interface{})["ref"].(string))
	assert.Equal(0, cs.Reads)
}

func TestFutureCompound(t *testing.T) {
	assert := assert.New(t)
	cs := &chunks.TestStore{}

	v := NewString("abc")
	resolved := futureFromValue(v)
	s := "sha1-a9993e364706816aba3e25717850c26c9cd0d89d"
	r := ref.MustParse(s)
	unresolved := futureFromRef(r)
	futures := []Future{resolved, unresolved}

	list := listFromFutures(futures, cs)
	assert.NotNil(list)
	m, err := getJSONList(list, cs)
	assert.NoError(err)
	assert.IsType([]interface{}{}, m.(map[string]interface{})["list"])
	assert.Equal(0, cs.Reads)

	set := setFromFutures(futures, cs)
	assert.NotNil(set)
	m, err = getJSONSet(set, cs)
	assert.NoError(err)
	assert.IsType([]interface{}{}, m.(map[string]interface{})["set"])
	assert.Equal(0, cs.Reads)

	mm := mapFromFutures(futures, cs)
	assert.NotNil(mm)
	m, err = getJSONMap(mm, cs)
	assert.NoError(err)
	assert.IsType([]interface{}{}, m.(map[string]interface{})["map"])
	assert.Equal(0, cs.Reads)
}

func TestCompoundBlobCodecChunked(t *testing.T) {
	assert := assert.New(t)
	cs := &chunks.MemoryStore{}

	f, err := os.Open("alice-short.txt")
	assert.NoError(err)
	defer f.Close()

	b, err := NewBlob(f)
	assert.NoError(err)
	cb, ok := b.(compoundBlob)
	assert.True(ok)

	r, err := jsonEncode(cb, cs)
	assert.Equal("sha1-981c8991b515a05f8c0a2058b306501ad833386e", r.String())

	reader, err := cs.Get(r)
	assert.NoError(err)
	v, err := jsonDecode(reader, cs)
	assert.NoError(err)
	assert.True(cb.Equals(v))
}
