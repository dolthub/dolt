package chunks

import (
	"bytes"
	"errors"
	"io/ioutil"
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestReadThroughStoreGet(t *testing.T) {
	assert := assert.New(t)

	bs := &TestStore{}

	// Prepopulate the backing store with "abc".
	input := "abc"
	w := bs.Put()
	_, err := w.Write([]byte(input))
	assert.NoError(err)
	ref, err := w.Ref()
	assert.NoError(err)

	// See http://www.di-mgt.com.au/sha_testvectors.html
	assert.Equal("sha1-a9993e364706816aba3e25717850c26c9cd0d89d", ref.String())

	assert.Equal(1, bs.Len())
	assert.Equal(1, bs.Writes)
	assert.Equal(0, bs.Reads)

	cs := &TestStore{}
	rts := NewReadThroughStore(cs, bs)

	// Now read "abc". It is not yet in the cache so we hit the backing store.
	reader, err := rts.Get(ref)
	assert.NoError(err)
	data, err := ioutil.ReadAll(reader)
	assert.NoError(err)
	assert.Equal(input, string(data))
	reader.Close()

	assert.Equal(1, bs.Len())
	assert.Equal(1, cs.Len())
	assert.Equal(1, cs.Writes)
	assert.Equal(1, bs.Writes)
	assert.Equal(1, cs.Reads)
	assert.Equal(1, bs.Reads)

	// Reading it again should not hit the backing store.
	reader, err = rts.Get(ref)
	assert.NoError(err)
	data, err = ioutil.ReadAll(reader)
	assert.NoError(err)
	assert.Equal(input, string(data))
	reader.Close()

	assert.Equal(1, bs.Len())
	assert.Equal(1, cs.Len())
	assert.Equal(1, cs.Writes)
	assert.Equal(1, bs.Writes)
	assert.Equal(2, cs.Reads)
	assert.Equal(1, bs.Reads)
}

func TestReadThroughStorePut(t *testing.T) {
	assert := assert.New(t)

	bs := &TestStore{}
	cs := &TestStore{}
	rts := NewReadThroughStore(cs, bs)

	// Storing "abc" should store it to both backing and caching store.
	input := "abc"
	w := rts.Put()
	_, err := w.Write([]byte(input))
	assert.NoError(err)
	ref, err := w.Ref()
	assert.NoError(err)

	// See http://www.di-mgt.com.au/sha_testvectors.html
	assert.Equal("sha1-a9993e364706816aba3e25717850c26c9cd0d89d", ref.String())

	assertInputInStore("abc", ref, bs, assert)
	assertInputInStore("abc", ref, cs, assert)
	assertInputInStore("abc", ref, rts, assert)
}

type failPutStore struct {
	MemoryStore
}

type failChunkWriter struct {
	memoryChunkWriter
}

func (w *failChunkWriter) Ref() (r ref.Ref, err error) {
	return ref.Ref{}, errors.New("Failed Ref")
}

func (s *failPutStore) Put() ChunkWriter {
	mcw := memoryChunkWriter{&s.MemoryStore, &bytes.Buffer{}, ref.Ref{}}
	return &failChunkWriter{mcw}
}

func TestReadThroughStorePutFails(t *testing.T) {
	assert := assert.New(t)

	bs := &failPutStore{MemoryStore{}}
	cs := &TestStore{}
	rts := NewReadThroughStore(cs, bs)

	// Storing "abc" should store it to both backing and caching store.
	input := "abc"
	w := rts.Put()
	_, err := w.Write([]byte(input))
	assert.NoError(err)
	_, err = w.Ref()
	assert.Error(err)

	// See http://www.di-mgt.com.au/sha_testvectors.html
	ref := ref.MustParse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")
	assertInputNotInStore("abc", ref, bs, assert)
	assertInputNotInStore("abc", ref, cs, assert)
	assertInputNotInStore("abc", ref, rts, assert)
}
