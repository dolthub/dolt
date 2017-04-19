// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/suite"
	"github.com/julienschmidt/httprouter"
)

const testAuthToken = "aToken123"

func TestHTTPChunkStore(t *testing.T) {
	suite.Run(t, &HTTPChunkStoreSuite{})
}

type HTTPChunkStoreSuite struct {
	suite.Suite
	cs    *chunks.TestStore
	store *httpChunkStore
}

type inlineServer struct {
	*httprouter.Router
}

func (serv inlineServer) Do(req *http.Request) (resp *http.Response, err error) {
	w := httptest.NewRecorder()
	serv.ServeHTTP(w, req)
	return &http.Response{
			StatusCode: w.Code,
			Status:     http.StatusText(w.Code),
			Header:     w.HeaderMap,
			Body:       ioutil.NopCloser(w.Body),
		},
		nil
}

func (suite *HTTPChunkStoreSuite) SetupTest() {
	suite.cs = chunks.NewTestStore()
	suite.store = NewHTTPChunkStoreForTest(suite.cs)
}

func NewHTTPChunkStoreForTest(cs chunks.ChunkStore) *httpChunkStore {
	serv := inlineServer{httprouter.New()}
	serv.POST(
		constants.WriteValuePath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			HandleWriteValue(w, req, ps, cs)
		},
	)
	serv.POST(
		constants.GetRefsPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			HandleGetRefs(w, req, ps, cs)
		},
	)
	serv.POST(
		constants.HasRefsPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			HandleHasRefs(w, req, ps, cs)
		},
	)
	serv.POST(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			HandleRootPost(w, req, ps, cs)
		},
	)
	serv.GET(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			HandleRootGet(w, req, ps, cs)
		},
	)
	return newHTTPChunkStoreWithClient("http://localhost:9000", "", serv)
}

func newAuthenticatingHTTPChunkStoreForTest(suite *HTTPChunkStoreSuite, hostUrl string) *httpChunkStore {
	authenticate := func(req *http.Request) {
		suite.Equal(testAuthToken, req.URL.Query().Get("access_token"))
	}

	serv := inlineServer{httprouter.New()}
	serv.POST(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			authenticate(req)
			HandleRootPost(w, req, ps, suite.cs)
		},
	)
	serv.GET(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			HandleRootGet(w, req, ps, suite.cs)
		},
	)
	return newHTTPChunkStoreWithClient(hostUrl, "", serv)
}

func newBadVersionHTTPChunkStoreForTest(suite *HTTPChunkStoreSuite) *httpChunkStore {
	serv := inlineServer{httprouter.New()}
	serv.POST(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			HandleRootPost(w, req, ps, suite.cs)
			w.Header().Set(NomsVersionHeader, "BAD")
		},
	)
	serv.GET(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			HandleRootGet(w, req, ps, suite.cs)
		},
	)
	return newHTTPChunkStoreWithClient("http://localhost", "", serv)
}

func (suite *HTTPChunkStoreSuite) TearDownTest() {
	suite.store.Close()
	suite.cs.Close()
}

func (suite *HTTPChunkStoreSuite) TestPutChunk() {
	c := types.EncodeValue(types.String("abc"), nil)
	suite.store.SchedulePut(c)
	suite.store.Flush()

	suite.Equal(1, suite.cs.Writes)
}

func (suite *HTTPChunkStoreSuite) TestPutChunksInOrder() {
	vals := []types.Value{
		types.String("abc"),
		types.String("def"),
	}
	l := types.NewList()
	for _, val := range vals {
		suite.store.SchedulePut(types.EncodeValue(val, nil))
		l = l.Append(types.NewRef(val))
	}
	suite.store.SchedulePut(types.EncodeValue(l, nil))
	suite.store.Flush()

	suite.Equal(3, suite.cs.Writes)
}

func (suite *HTTPChunkStoreSuite) TestRebase() {
	suite.Equal(hash.Hash{}, suite.store.Root())
	c := types.EncodeValue(types.NewMap(), nil)
	suite.cs.Put(c)
	suite.True(suite.cs.Commit(c.Hash(), hash.Hash{})) // change happens behind our backs
	suite.Equal(hash.Hash{}, suite.store.Root())       // shouldn't be visible yet

	suite.store.Rebase()
	suite.Equal(c.Hash(), suite.cs.Root())
}

func (suite *HTTPChunkStoreSuite) TestRoot() {
	c := types.EncodeValue(types.NewMap(), nil)
	suite.cs.Put(c)
	suite.True(suite.store.Commit(c.Hash(), hash.Hash{}))
	suite.Equal(c.Hash(), suite.cs.Root())
}

func (suite *HTTPChunkStoreSuite) TestVersionMismatch() {
	store := newBadVersionHTTPChunkStoreForTest(suite)
	defer store.Close()
	c := types.EncodeValue(types.NewMap(), nil)
	suite.cs.Put(c)
	suite.Panics(func() { store.Commit(c.Hash(), hash.Hash{}) })
}

func (suite *HTTPChunkStoreSuite) TestCommit() {
	c := types.EncodeValue(types.NewMap(), nil)
	suite.cs.Put(c)
	suite.True(suite.store.Commit(c.Hash(), hash.Hash{}))
	suite.Equal(c.Hash(), suite.cs.Root())
}

func (suite *HTTPChunkStoreSuite) TestCommitWithParams() {
	u := fmt.Sprintf("http://localhost:9000?access_token=%s&other=19", testAuthToken)
	store := newAuthenticatingHTTPChunkStoreForTest(suite, u)
	defer store.Close()
	c := types.EncodeValue(types.NewMap(), nil)
	suite.cs.Put(c)
	suite.True(store.Commit(c.Hash(), hash.Hash{}))
	suite.Equal(c.Hash(), suite.cs.Root())
}

func (suite *HTTPChunkStoreSuite) TestGet() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	suite.cs.PutMany(chnx)
	got := suite.store.Get(chnx[0].Hash())
	suite.Equal(chnx[0].Hash(), got.Hash())
	got = suite.store.Get(chnx[1].Hash())
	suite.Equal(chnx[1].Hash(), got.Hash())
}

func (suite *HTTPChunkStoreSuite) TestGetMany() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	notPresent := chunks.NewChunk([]byte("ghi")).Hash()
	suite.cs.PutMany(chnx)
	suite.cs.Flush()

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash(), notPresent)
	foundChunks := make(chan *chunks.Chunk)
	go func() { suite.store.GetMany(hashes, foundChunks); close(foundChunks) }()

	for c := range foundChunks {
		hashes.Remove(c.Hash())
	}
	suite.Len(hashes, 1)
	suite.True(hashes.Has(notPresent))
}

func (suite *HTTPChunkStoreSuite) TestGetManyAllCached() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	suite.store.SchedulePut(chnx[0])
	suite.store.SchedulePut(chnx[1])

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash())
	foundChunks := make(chan *chunks.Chunk)
	go func() { suite.store.GetMany(hashes, foundChunks); close(foundChunks) }()

	for c := range foundChunks {
		hashes.Remove(c.Hash())
	}
	suite.Len(hashes, 0)
}

func (suite *HTTPChunkStoreSuite) TestGetManySomeCached() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	cached := chunks.NewChunk([]byte("ghi"))
	suite.cs.PutMany(chnx)
	suite.cs.Flush()
	suite.store.Put(cached)

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash(), cached.Hash())
	foundChunks := make(chan *chunks.Chunk)
	go func() { suite.store.GetMany(hashes, foundChunks); close(foundChunks) }()

	for c := range foundChunks {
		hashes.Remove(c.Hash())
	}
	suite.Len(hashes, 0)
}

func (suite *HTTPChunkStoreSuite) TestGetSame() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("def")),
		chunks.NewChunk([]byte("def")),
	}
	suite.cs.PutMany(chnx)
	got := suite.store.Get(chnx[0].Hash())
	suite.Equal(chnx[0].Hash(), got.Hash())
	got = suite.store.Get(chnx[1].Hash())
	suite.Equal(chnx[1].Hash(), got.Hash())
}

func (suite *HTTPChunkStoreSuite) TestHas() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	suite.cs.PutMany(chnx)
	suite.True(suite.store.Has(chnx[0].Hash()))
	suite.True(suite.store.Has(chnx[1].Hash()))
}

func (suite *HTTPChunkStoreSuite) TestHasMany() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	suite.cs.PutMany(chnx)
	suite.cs.Flush()
	notPresent := chunks.NewChunk([]byte("ghi")).Hash()

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash(), notPresent)
	present := suite.store.HasMany(hashes)

	suite.Len(present, len(chnx))
	for _, c := range chnx {
		suite.True(present.Has(c.Hash()), "%s not present in %v", c.Hash(), present)
	}
	suite.False(present.Has(notPresent))
}

func (suite *HTTPChunkStoreSuite) TestHasManyAllCached() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	suite.store.PutMany(chnx)

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash())
	present := suite.store.HasMany(hashes)

	suite.Len(present, len(chnx))
	for _, c := range chnx {
		suite.True(present.Has(c.Hash()), "%s not present in %v", c.Hash(), present)
	}
}

func (suite *HTTPChunkStoreSuite) TestHasManySomeCached() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	cached := chunks.NewChunk([]byte("ghi"))
	suite.cs.PutMany(chnx)
	suite.cs.Flush()
	suite.store.Put(cached)

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash(), cached.Hash())
	present := suite.store.HasMany(hashes)

	suite.Len(present, len(chnx)+1)
	for _, c := range chnx {
		suite.True(present.Has(c.Hash()), "%s not present in %v", c.Hash(), present)
	}
}
