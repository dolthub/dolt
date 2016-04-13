package datas

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

func TestCheckChunksInCache(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	cvs := naiveCachingValueStore(cs)

	b := types.NewEmptyBlob()
	cs.Put(types.EncodeValue(b, nil))
	cvs.set(b.Ref(), presentChunk(b.Type()))

	bref := types.NewRefOfBlob(b.Ref())
	assert.NotPanics(func() { cvs.checkChunksInCache(bref) })
}

func naiveCachingValueStore(cs chunks.ChunkStore) cachingValueStore {
	return newCachingValueStore(&naiveHintedChunkStore{cs})
}

func TestCacheOnReadValue(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	cvs := naiveCachingValueStore(cs)

	b := types.NewEmptyBlob()
	bref := cvs.WriteValue(b).(types.RefOfBlob)
	r := cvs.WriteValue(bref)

	cvs2 := naiveCachingValueStore(cs)
	v := cvs2.ReadValue(r.TargetRef())
	assert.True(bref.Equals(v))
	assert.True(cvs2.isPresent(b.Ref()))
	assert.True(cvs2.isPresent(bref.Ref()))
}

func TestHintsOnCache(t *testing.T) {
	assert := assert.New(t)
	cvs := naiveCachingValueStore(chunks.NewTestStore())

	bs := []types.Blob{types.NewEmptyBlob(), types.NewBlob(bytes.NewBufferString("f"))}
	l := types.NewList()
	for _, b := range bs {
		bref := cvs.WriteValue(b).(types.RefOfBlob)
		l = l.Append(bref)
	}
	r := cvs.WriteValue(l)

	v := cvs.ReadValue(r.TargetRef())
	if assert.True(l.Equals(v)) {
		l = v.(types.List)
		bref := cvs.WriteValue(types.NewBlob(bytes.NewBufferString("g"))).(types.RefOfBlob)
		l = l.Insert(0, bref)

		hints := cvs.checkChunksInCache(l)
		if assert.Len(hints, 1) {
			_, present := hints[v.Ref()]
			assert.True(present)
		}
	}
}
