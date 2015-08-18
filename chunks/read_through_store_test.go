package chunks

import (
	"io/ioutil"
	"testing"

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
	ref := w.Ref()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	assert.Equal("sha1-a9993e364706816aba3e25717850c26c9cd0d89d", ref.String())

	assert.Equal(1, bs.Len())
	assert.Equal(1, bs.Writes)
	assert.Equal(0, bs.Reads)

	cs := &TestStore{}
	rts := NewReadThroughStore(cs, bs)

	// Now read "abc". It is not yet in the cache so we hit the backing store.
	reader := rts.Get(ref)
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
	reader = rts.Get(ref)
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
	ref := w.Ref()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	assert.Equal("sha1-a9993e364706816aba3e25717850c26c9cd0d89d", ref.String())

	assertInputInStore("abc", ref, bs, assert)
	assertInputInStore("abc", ref, cs, assert)
	assertInputInStore("abc", ref, rts, assert)
}
