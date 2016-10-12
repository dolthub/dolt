// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
	"github.com/golang/snappy"
)

func TestHandleWriteValue(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	ds := NewDatabase(cs)

	l := types.NewList(
		ds.WriteValue(types.Bool(true)),
		ds.WriteValue(types.Bool(false)),
	)
	ds.WriteValue(l)

	hint := l.Hash()
	newItem := types.NewEmptyBlob()
	itemChunk := types.EncodeValue(newItem, nil)
	l2 := l.Insert(1, types.NewRef(newItem))
	listChunk := types.EncodeValue(l2, nil)

	body := &bytes.Buffer{}
	serializeHints(body, map[hash.Hash]struct{}{hint: {}})
	chunks.Serialize(itemChunk, body)
	chunks.Serialize(listChunk, body)

	w := httptest.NewRecorder()
	HandleWriteValue(w, newRequest("POST", "", "", body, nil), params{}, cs)

	if assert.Equal(http.StatusCreated, w.Code, "Handler error:\n%s", string(w.Body.Bytes())) {
		ds2 := NewDatabase(cs)
		v := ds2.ReadValue(l2.Hash())
		if assert.NotNil(v) {
			assert.True(v.Equals(l2), "%+v != %+v", v, l2)
		}
	}
}

func TestHandleWriteValueDupChunks(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()

	newItem := types.NewEmptyBlob()
	itemChunk := types.EncodeValue(newItem, nil)

	body := &bytes.Buffer{}
	serializeHints(body, map[hash.Hash]struct{}{})
	// Write the same chunk to body enough times to be certain that at least one of the concurrent deserialize/decode passes completes before the last one can continue.
	for i := 0; i <= writeValueConcurrency; i++ {
		chunks.Serialize(itemChunk, body)
	}

	w := httptest.NewRecorder()
	HandleWriteValue(w, newRequest("POST", "", "", body, nil), params{}, cs)

	if assert.Equal(http.StatusCreated, w.Code, "Handler error:\n%s", string(w.Body.Bytes())) {
		ds2 := NewDatabase(cs)
		v := ds2.ReadValue(newItem.Hash())
		if assert.NotNil(v) {
			assert.True(v.Equals(newItem), "%+v != %+v", v, newItem)
		}
	}
}

func TestHandleWriteValueBackpressure(t *testing.T) {
	assert := assert.New(t)
	cs := &backpressureCS{ChunkStore: chunks.NewMemoryStore()}
	ds := NewDatabase(cs)

	l := types.NewList(
		ds.WriteValue(types.Bool(true)),
		ds.WriteValue(types.Bool(false)),
	)
	ds.WriteValue(l)

	hint := l.Hash()
	newItem := types.NewEmptyBlob()
	itemChunk := types.EncodeValue(newItem, nil)
	l2 := l.Insert(1, types.NewRef(newItem))
	listChunk := types.EncodeValue(l2, nil)

	body := &bytes.Buffer{}
	serializeHints(body, map[hash.Hash]struct{}{hint: {}})
	chunks.Serialize(itemChunk, body)
	chunks.Serialize(listChunk, body)

	w := httptest.NewRecorder()
	HandleWriteValue(w, newRequest("POST", "", "", body, nil), params{}, cs)

	if assert.Equal(httpStatusTooManyRequests, w.Code, "Handler error:\n%s", string(w.Body.Bytes())) {
		hashes := deserializeHashes(w.Body)
		assert.Len(hashes, 1)
		assert.Equal(l2.Hash(), hashes[0])
	}
}

func TestBuildWriteValueRequest(t *testing.T) {
	assert := assert.New(t)
	input1, input2 := "abc", "def"
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte(input1)),
		chunks.NewChunk([]byte(input2)),
	}

	inChunkChan := make(chan *chunks.Chunk, 2)
	inChunkChan <- &chnx[0]
	inChunkChan <- &chnx[1]
	close(inChunkChan)

	hints := map[hash.Hash]struct{}{
		hash.Parse("00000000000000000000000000000002"): {},
		hash.Parse("00000000000000000000000000000003"): {},
	}
	compressed := buildWriteValueRequest(inChunkChan, hints)
	gr := snappy.NewReader(compressed)

	count := 0
	for hint := range deserializeHints(gr) {
		count++
		_, present := hints[hint]
		assert.True(present)
	}
	assert.Equal(len(hints), count)

	outChunkChan := make(chan interface{}, 16)
	go chunks.DeserializeToChan(gr, outChunkChan)
	for c := range outChunkChan {
		assert.Equal(chnx[0].Hash(), c.(*chunks.Chunk).Hash())
		chnx = chnx[1:]
	}
	assert.Empty(chnx)
}

func serializeChunks(chnx []chunks.Chunk, assert *assert.Assertions) io.Reader {
	body := &bytes.Buffer{}
	sw := snappy.NewBufferedWriter(body)
	for _, chunk := range chnx {
		chunks.Serialize(chunk, sw)
	}
	assert.NoError(sw.Close())
	return body
}

func TestBuildHashesRequest(t *testing.T) {
	assert := assert.New(t)
	hashes := map[hash.Hash]struct{}{
		hash.Parse("00000000000000000000000000000002"): {},
		hash.Parse("00000000000000000000000000000003"): {},
	}
	r := buildHashesRequest(hashes)
	b, err := ioutil.ReadAll(r)
	assert.NoError(err)

	urlValues, err := url.ParseQuery(string(b))
	assert.NoError(err)
	assert.NotEmpty(urlValues)

	queryRefs := urlValues["ref"]
	assert.Len(queryRefs, len(hashes))
	for _, r := range queryRefs {
		_, present := hashes[hash.Parse(r)]
		assert.True(present, "Query contains %s, which is not in initial refs", r)
	}
}

func TestHandleGetRefs(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	input1, input2 := "abc", "def"
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte(input1)),
		chunks.NewChunk([]byte(input2)),
	}
	err := cs.PutMany(chnx)
	assert.NoError(err)

	body := strings.NewReader(fmt.Sprintf("ref=%s&ref=%s", chnx[0].Hash(), chnx[1].Hash()))

	w := httptest.NewRecorder()
	HandleGetRefs(
		w,
		newRequest("POST", "", "", body, http.Header{
			"Content-Type": {"application/x-www-form-urlencoded"},
		}),
		params{},
		cs,
	)

	if assert.Equal(http.StatusOK, w.Code, "Handler error:\n%s", string(w.Body.Bytes())) {
		chunkChan := make(chan interface{})
		go chunks.DeserializeToChan(w.Body, chunkChan)
		for c := range chunkChan {
			assert.Equal(chnx[0].Hash(), c.(*chunks.Chunk).Hash())
			chnx = chnx[1:]
		}
		assert.Empty(chnx)
	}
}

func TestHandleHasRefs(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	input1, input2 := "abc", "def"
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte(input1)),
		chunks.NewChunk([]byte(input2)),
	}
	err := cs.PutMany(chnx)
	assert.NoError(err)

	absent := hash.Parse("00000000000000000000000000000002")
	body := strings.NewReader(fmt.Sprintf("ref=%s&ref=%s&ref=%s", chnx[0].Hash(), chnx[1].Hash(), absent))

	w := httptest.NewRecorder()
	HandleHasRefs(
		w,
		newRequest("POST", "", "", body, http.Header{
			"Content-Type": {"application/x-www-form-urlencoded"},
		}),
		params{},
		cs,
	)

	if assert.Equal(http.StatusOK, w.Code, "Handler error:\n%s", string(w.Body.Bytes())) {
		scanner := bufio.NewScanner(w.Body)
		scanner.Split(bufio.ScanWords)
		for scanner.Scan() {
			h := hash.Parse(scanner.Text())
			scanner.Scan()
			if scanner.Text() == "true" {
				assert.Equal(chnx[0].Hash(), h)
				chnx = chnx[1:]
			} else {
				assert.Equal(absent, h)
			}
		}
		assert.Empty(chnx)
	}
}

func TestHandleGetRoot(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	c := chunks.NewChunk([]byte("abc"))
	cs.Put(c)
	assert.True(cs.UpdateRoot(c.Hash(), hash.Hash{}))

	w := httptest.NewRecorder()
	HandleRootGet(w, newRequest("GET", "", "", nil, nil), params{}, cs)

	if assert.Equal(http.StatusOK, w.Code, "Handler error:\n%s", string(w.Body.Bytes())) {
		root := hash.Parse(string(w.Body.Bytes()))
		assert.Equal(c.Hash(), root)
	}
}

func TestHandleGetBase(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	c := chunks.NewChunk([]byte("abc"))
	cs.Put(c)
	assert.True(cs.UpdateRoot(c.Hash(), hash.Hash{}))

	w := httptest.NewRecorder()
	HandleBaseGet(w, newRequest("GET", "", "", nil, nil), params{}, cs)

	if assert.Equal(http.StatusOK, w.Code, "Handler error:\n%s", string(w.Body.Bytes())) {
		assert.Equal([]byte(nomsBaseHTML), w.Body.Bytes())
	}
}

func TestHandlePostRoot(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()
	vs := types.NewValueStore(types.NewBatchStoreAdaptor(cs))

	commit := NewCommit(types.String("head"), types.NewSet(), types.NewStruct("Meta", types.StructData{}))
	newHead := types.NewMap(types.String("dataset1"), vs.WriteValue(commit))
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		types.EncodeValue(newHead, nil),
	}
	err := cs.PutMany(chnx)
	assert.NoError(err)

	// First attempt should fail, as 'last' won't match.
	u := &url.URL{}
	queryParams := url.Values{}
	queryParams.Add("last", chnx[0].Hash().String())
	queryParams.Add("current", chnx[1].Hash().String())
	u.RawQuery = queryParams.Encode()
	url := u.String()

	w := httptest.NewRecorder()
	HandleRootPost(w, newRequest("POST", "", url, nil, nil), params{}, cs)
	assert.Equal(http.StatusConflict, w.Code, "Handler error:\n%s", string(w.Body.Bytes()))

	// Now, update the root manually to 'last' and try again.
	assert.True(cs.UpdateRoot(chnx[0].Hash(), hash.Hash{}))
	w = httptest.NewRecorder()
	HandleRootPost(w, newRequest("POST", "", url, nil, nil), params{}, cs)
	assert.Equal(http.StatusOK, w.Code, "Handler error:\n%s", string(w.Body.Bytes()))
}

func TestRejectPostRoot(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewTestStore()

	newHead := types.NewMap(types.String("dataset1"), types.String("Not a Head"))
	chunk := types.EncodeValue(newHead, nil)
	cs.Put(chunk)

	// First attempt should fail, as 'last' won't match.
	u := &url.URL{}
	queryParams := url.Values{}
	queryParams.Add("last", chunks.EmptyChunk.Hash().String())
	queryParams.Add("current", chunk.Hash().String())
	u.RawQuery = queryParams.Encode()
	url := u.String()

	w := httptest.NewRecorder()
	HandleRootPost(w, newRequest("POST", "", url, nil, nil), params{}, cs)
	assert.Equal(http.StatusBadRequest, w.Code, "Handler error:\n%s", string(w.Body.Bytes()))
}

type params map[string]string

func (p params) ByName(k string) string {
	return p[k]
}
