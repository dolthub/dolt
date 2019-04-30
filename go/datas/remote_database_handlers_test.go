// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"bufio"
	"bytes"
	"context"
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
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
)

func TestHandleWriteValue(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewView())

	l := types.NewList(context.Background(),
		db,
		db.WriteValue(context.Background(), types.Bool(true)),
		db.WriteValue(context.Background(), types.Bool(false)),
	)
	r := db.WriteValue(context.Background(), l)
	_, err := db.CommitValue(context.Background(), db.GetDataset(context.Background(), "datasetID"), r)
	assert.NoError(err)

	newItem := types.NewEmptyBlob(db)
	itemChunk := types.EncodeValue(newItem)
	l2 := l.Edit().Insert(1, types.NewRef(newItem)).List(context.Background())
	listChunk := types.EncodeValue(l2)

	body := &bytes.Buffer{}
	chunks.Serialize(itemChunk, body)
	chunks.Serialize(listChunk, body)

	w := httptest.NewRecorder()
	HandleWriteValue(w, newRequest("POST", "", "", body, nil), params{}, storage.NewView())

	if assert.Equal(http.StatusCreated, w.Code, "Handler error:\n%s", string(w.Body.Bytes())) {
		db2 := NewDatabase(storage.NewView())
		v := db2.ReadValue(context.Background(), l2.Hash())
		if assert.NotNil(v) {
			assert.True(v.Equals(l2), "%+v != %+v", v, l2)
		}
	}
}

func TestHandleWriteValuePanic(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}

	body := &bytes.Buffer{}
	body.WriteString("Bogus")

	w := httptest.NewRecorder()
	HandleWriteValue(w, newRequest("POST", "", "", body, nil), params{}, storage.NewView())

	assert.Equal(http.StatusBadRequest, w.Code, "Handler error:\n%s", string(w.Body.Bytes()))
}

func TestHandleWriteValueDupChunks(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}
	db := NewDatabase(storage.NewView())
	defer db.Close()

	newItem := types.NewEmptyBlob(db)
	itemChunk := types.EncodeValue(newItem)

	body := &bytes.Buffer{}
	// Write the same chunk to body enough times to be certain that at least one of the concurrent deserialize/decode passes completes before the last one can continue.
	for i := 0; i <= writeValueConcurrency; i++ {
		chunks.Serialize(itemChunk, body)
	}

	w := httptest.NewRecorder()
	HandleWriteValue(w, newRequest("POST", "", "", body, nil), params{}, storage.NewView())

	if assert.Equal(http.StatusCreated, w.Code, "Handler error:\n%s", string(w.Body.Bytes())) {
		db := NewDatabase(storage.NewView())
		v := db.ReadValue(context.Background(), newItem.Hash())
		if assert.NotNil(v) {
			assert.True(v.Equals(newItem), "%+v != %+v", v, newItem)
		}
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

	compressed := buildWriteValueRequest(inChunkChan)
	gr := snappy.NewReader(compressed)

	outChunkChan := make(chan *chunks.Chunk, len(chnx))
	chunks.Deserialize(gr, outChunkChan)
	close(outChunkChan)

	for c := range outChunkChan {
		assert.Equal(chnx[0].Hash(), c.Hash())
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
	batch := chunks.ReadBatch{
		hash.Parse("00000000000000000000000000000002"): nil,
		hash.Parse("00000000000000000000000000000003"): nil,
	}
	r := buildHashesRequest(batch)
	defer r.Close()
	requested := deserializeHashes(r)

	for _, h := range requested {
		_, present := batch[h]
		assert.True(present, "Query contains %s, which is not in initial refs", h)
	}
}

func TestHandleGetRefs(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}
	cs := storage.NewView()
	input1, input2 := "abc", "def"
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte(input1)),
		chunks.NewChunk([]byte(input2)),
	}
	for _, c := range chnx {
		cs.Put(context.Background(), c)
	}
	persistChunks(context.Background(), cs)

	body := buildHashesRequest(chunks.ReadBatch{chnx[0].Hash(): nil, chnx[1].Hash(): nil})

	w := httptest.NewRecorder()
	HandleGetRefs(
		w,
		newRequest("POST", "", "", body, http.Header{
			"Content-Type": {"application/octet-stream"},
		}),
		params{},
		storage.NewView(),
	)

	if assert.Equal(http.StatusOK, w.Code, "Handler error:\n%s", string(w.Body.Bytes())) {
		chunkChan := make(chan *chunks.Chunk, len(chnx))
		chunks.Deserialize(w.Body, chunkChan)
		close(chunkChan)

		foundHashes := hash.HashSet{}
		for c := range chunkChan {
			foundHashes[c.Hash()] = struct{}{}
		}

		assert.True(len(foundHashes) == 2)
		_, hasC1 := foundHashes[chnx[0].Hash()]
		assert.True(hasC1)
		_, hasC2 := foundHashes[chnx[1].Hash()]
		assert.True(hasC2)
	}
}

func TestHandleGetBlob(t *testing.T) {
	assert := assert.New(t)

	blobContents := "I am a blob"
	storage := &chunks.MemoryStorage{}
	db := NewDatabase(storage.NewView())
	ds := db.GetDataset(context.Background(), "foo")

	// Test missing h
	w := httptest.NewRecorder()
	HandleGetBlob(
		w,
		newRequest("GET", "", "/getBlob/", strings.NewReader(""), http.Header{}),
		params{},
		storage.NewView(),
	)
	assert.Equal(http.StatusBadRequest, w.Code, "Handler error:\n%s", string(w.Body.Bytes()))

	b := types.NewBlob(context.Background(), db, bytes.NewBuffer([]byte(blobContents)))

	// Test non-present hash
	w = httptest.NewRecorder()
	HandleGetBlob(
		w,
		newRequest("GET", "", fmt.Sprintf("/getBlob/?h=%s", b.Hash().String()), strings.NewReader(""), http.Header{}),
		params{},
		storage.NewView(),
	)
	assert.Equal(http.StatusBadRequest, w.Code, "Handler error:\n%s", string(w.Body.Bytes()))

	r := db.WriteValue(context.Background(), b)
	ds, err := db.CommitValue(context.Background(), ds, r)
	assert.NoError(err)

	// Valid
	w = httptest.NewRecorder()
	HandleGetBlob(
		w,
		newRequest("GET", "", fmt.Sprintf("/getBlob/?h=%s", r.TargetHash().String()), strings.NewReader(""), http.Header{}),
		params{},
		storage.NewView(),
	)

	if assert.Equal(http.StatusOK, w.Code, "Handler error:\n%s", string(w.Body.Bytes())) {
		out, _ := ioutil.ReadAll(w.Body)
		assert.Equal(string(out), blobContents)
	}

	// Test non-blob
	r2 := db.WriteValue(context.Background(), types.Float(1))
	_, err = db.CommitValue(context.Background(), ds, r2)
	assert.NoError(err)

	w = httptest.NewRecorder()
	HandleGetBlob(
		w,
		newRequest("GET", "", fmt.Sprintf("/getBlob/?h=%s", r2.TargetHash().String()), strings.NewReader(""), http.Header{}),
		params{},
		storage.NewView(),
	)
	assert.Equal(http.StatusBadRequest, w.Code, "Handler error:\n%s", string(w.Body.Bytes()))
}

func TestHandleHasRefs(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}
	input1, input2, input3 := "abc", "def", "ghi"
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte(input1)),
		chunks.NewChunk([]byte(input2)),
	}
	present := chunks.NewChunk([]byte(input3))
	cs := storage.NewView()
	cs.Put(context.Background(), present)
	persistChunks(context.Background(), cs)

	body := buildHashesRequest(chunks.ReadBatch{
		chnx[0].Hash(): nil,
		chnx[1].Hash(): nil,
		present.Hash(): nil,
	})

	w := httptest.NewRecorder()
	HandleHasRefs(
		w,
		newRequest("POST", "", "", body, http.Header{
			"Content-Type": {"application/octet-stream"},
		}),
		params{},
		storage.NewView(),
	)

	absent := hash.HashSet{}
	if assert.Equal(http.StatusOK, w.Code, "Handler error:\n%s", string(w.Body.Bytes())) {
		scanner := bufio.NewScanner(w.Body)
		scanner.Split(bufio.ScanWords)
		for scanner.Scan() {
			absent.Insert(hash.Parse(scanner.Text()))
		}
	}
	if assert.Len(absent, len(chnx)) {
		for _, c := range chnx {
			assert.True(absent.Has(c.Hash()))
		}
		assert.False(absent.Has(present.Hash()))
	}
}

func TestHandleGetRoot(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}
	cs := storage.NewView()
	c := chunks.NewChunk([]byte("abc"))
	cs.Put(context.Background(), c)
	assert.True(cs.Commit(context.Background(), c.Hash(), hash.Hash{}))

	w := httptest.NewRecorder()
	HandleRootGet(w, newRequest("GET", "", "", nil, nil), params{}, storage.NewView())

	if assert.Equal(http.StatusOK, w.Code, "Handler error:\n%s", string(w.Body.Bytes())) {
		root := hash.Parse(string(w.Body.Bytes()))
		assert.Equal(c.Hash(), root)
	}
}

func TestHandleGetBase(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}

	w := httptest.NewRecorder()
	HandleBaseGet(w, newRequest("GET", "", "", nil, nil), params{}, storage.NewView())

	if assert.Equal(http.StatusOK, w.Code, "Handler error:\n%s", string(w.Body.Bytes())) {
		assert.Equal([]byte(nomsBaseHTML), w.Body.Bytes())
	}
}

func TestHandlePostRoot(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}
	cs := storage.NewView()
	vs := types.NewValueStore(cs)

	validate := func(code int, root hash.Hash, w *httptest.ResponseRecorder) {
		assert.Equal(code, w.Code, "Handler error:\n%s", string(w.Body.Bytes()))
		assert.Equal(root, hash.Parse(string(w.Body.Bytes())))
	}

	// Empty -> Empty should be OK.
	url := buildPostRootURL(hash.Hash{}, hash.Hash{})
	w := httptest.NewRecorder()
	HandleRootPost(w, newRequest("POST", "", url, nil, nil), params{}, storage.NewView())
	validate(http.StatusOK, hash.Hash{}, w)

	commit := buildTestCommit(vs, types.String("head"))
	commitRef := vs.WriteValue(context.Background(), commit)
	firstHead := types.NewMap(context.Background(), vs, types.String("dataset1"), types.ToRefOfValue(commitRef))
	firstHeadRef := vs.WriteValue(context.Background(), firstHead)
	vs.Commit(context.Background(), vs.Root(context.Background()), vs.Root(context.Background()))

	commit = buildTestCommit(vs, types.String("second"), commitRef)
	newHead := types.NewMap(context.Background(), vs, types.String("dataset1"), types.ToRefOfValue(vs.WriteValue(context.Background(), commit)))
	newHeadRef := vs.WriteValue(context.Background(), newHead)
	vs.Commit(context.Background(), vs.Root(context.Background()), vs.Root(context.Background()))

	// First attempt should fail, as 'last' won't match.
	url = buildPostRootURL(newHeadRef.TargetHash(), firstHeadRef.TargetHash())
	w = httptest.NewRecorder()
	HandleRootPost(w, newRequest("POST", "", url, nil, nil), params{}, storage.NewView())
	validate(http.StatusConflict, hash.Hash{}, w)

	// Now, update the root manually to 'last' and try again.
	assert.True(cs.Commit(context.Background(), firstHeadRef.TargetHash(), hash.Hash{}))
	w = httptest.NewRecorder()
	HandleRootPost(w, newRequest("POST", "", url, nil, nil), params{}, storage.NewView())
	validate(http.StatusOK, newHeadRef.TargetHash(), w)
}

func buildPostRootURL(current, last hash.Hash) string {
	u := &url.URL{}
	queryParams := url.Values{}
	queryParams.Add("last", last.String())
	queryParams.Add("current", current.String())
	u.RawQuery = queryParams.Encode()
	return u.String()
}

func buildTestCommit(vrw types.ValueReadWriter, v types.Value, parents ...types.Value) types.Struct {
	return NewCommit(v, types.NewSet(context.Background(), vrw, parents...), types.NewStruct("Meta", types.StructData{}))
}

func TestRejectPostRoot(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}
	cs := storage.NewView()
	vs := types.NewValueStore(cs)
	defer vs.Close()

	newHead := types.NewMap(context.Background(), vs, types.String("dataset1"), types.String("Not a Head"))
	chunk := types.EncodeValue(newHead)
	cs.Put(context.Background(), chunk)
	persistChunks(context.Background(), cs)

	// Attempt should fail, as newHead isn't the right type.
	url := buildPostRootURL(chunk.Hash(), hash.Hash{})
	w := httptest.NewRecorder()
	HandleRootPost(w, newRequest("POST", "", url, nil, nil), params{}, storage.NewView())
	assert.Equal(http.StatusBadRequest, w.Code, "Handler error:\n%s", string(w.Body.Bytes()))

	// Put in a legit commit
	commit := buildTestCommit(vs, types.String("commit"))
	head := types.NewMap(context.Background(), vs, types.String("dataset1"), types.ToRefOfValue(vs.WriteValue(context.Background(), commit)))
	headRef := vs.WriteValue(context.Background(), head)
	assert.True(vs.Commit(context.Background(), headRef.TargetHash(), vs.Root(context.Background())))

	// Attempt to update head to empty hash should fail
	url = buildPostRootURL(hash.Hash{}, headRef.TargetHash())
	w = httptest.NewRecorder()
	HandleRootPost(w, newRequest("POST", "", url, nil, nil), params{}, storage.NewView())
	assert.Equal(http.StatusBadRequest, w.Code, "Handler error:\n%s", string(w.Body.Bytes()))

	// Attempt to update from a non-present chunks should fail
	url = buildPostRootURL(headRef.TargetHash(), chunks.EmptyChunk.Hash())
	w = httptest.NewRecorder()
	HandleRootPost(w, newRequest("POST", "", url, nil, nil), params{}, storage.NewView())
	assert.Equal(http.StatusBadRequest, w.Code, "Handler error:\n%s", string(w.Body.Bytes()))
}

type params map[string]string

func (p params) ByName(k string) string {
	return p[k]
}
