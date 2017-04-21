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
	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"
	"github.com/julienschmidt/httprouter"
)

const testAuthToken = "aToken123"

func TestHTTPChunkStore(t *testing.T) {
	suite.Run(t, &HTTPChunkStoreSuite{})
}

type HTTPChunkStoreSuite struct {
	suite.Suite
	serverCS *chunks.TestStoreView
	http     *httpChunkStore
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
	storage := &chunks.TestStorage{}
	suite.serverCS = storage.NewView()
	suite.http = newHTTPChunkStoreForTest(suite.serverCS)
}

func newHTTPChunkStoreForTest(cs chunks.ChunkStore) *httpChunkStore {
	// Ideally, this function (and its bretheren below) would take a *TestStorage and mint a fresh TestStoreView in each handler call below. That'd break a bunch of tests in pull_test.go that want to pass in a single TestStoreView and then inspect it after doing a bunch of work. The cs.Rebase() calls here are a good compromise for now, but BUG 3415 tracks Making This Right.
	serv := inlineServer{httprouter.New()}
	serv.POST(
		constants.WriteValuePath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase()
			HandleWriteValue(w, req, ps, cs)
		},
	)
	serv.POST(
		constants.GetRefsPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase()
			HandleGetRefs(w, req, ps, cs)
		},
	)
	serv.POST(
		constants.HasRefsPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase()
			HandleHasRefs(w, req, ps, cs)
		},
	)
	serv.POST(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase()
			HandleRootPost(w, req, ps, cs)
		},
	)
	serv.GET(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase()
			HandleRootGet(w, req, ps, cs)
		},
	)
	return newHTTPChunkStoreWithClient("http://localhost:9000", "", serv)
}

func newAuthenticatingHTTPChunkStoreForTest(assert *assert.Assertions, cs chunks.ChunkStore, hostUrl string) *httpChunkStore {
	authenticate := func(req *http.Request) {
		assert.Equal(testAuthToken, req.URL.Query().Get("access_token"))
	}

	serv := inlineServer{httprouter.New()}
	serv.POST(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase()
			authenticate(req)
			HandleRootPost(w, req, ps, cs)
		},
	)
	serv.GET(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase()
			HandleRootGet(w, req, ps, cs)
		},
	)
	return newHTTPChunkStoreWithClient(hostUrl, "", serv)
}

func newBadVersionHTTPChunkStoreForTest(cs chunks.ChunkStore) *httpChunkStore {
	serv := inlineServer{httprouter.New()}
	serv.POST(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase()
			HandleRootPost(w, req, ps, cs)
			w.Header().Set(NomsVersionHeader, "BAD")
		},
	)
	serv.GET(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase()
			HandleRootGet(w, req, ps, cs)
		},
	)
	return newHTTPChunkStoreWithClient("http://localhost", "", serv)
}

func (suite *HTTPChunkStoreSuite) TearDownTest() {
	suite.http.Close()
	suite.serverCS.Close()
}

func (suite *HTTPChunkStoreSuite) TestPutChunk() {
	c := types.EncodeValue(types.String("abc"), nil)
	suite.http.Put(c)
	suite.http.Flush()

	suite.Equal(1, suite.serverCS.Writes)
}

func (suite *HTTPChunkStoreSuite) TestPutChunksInOrder() {
	vals := []types.Value{
		types.String("abc"),
		types.String("def"),
	}
	l := types.NewList()
	for _, val := range vals {
		suite.http.Put(types.EncodeValue(val, nil))
		l = l.Append(types.NewRef(val))
	}
	suite.http.Put(types.EncodeValue(l, nil))
	suite.http.Flush()

	suite.Equal(3, suite.serverCS.Writes)
}

func (suite *HTTPChunkStoreSuite) TestRebase() {
	suite.Equal(hash.Hash{}, suite.http.Root())
	c := types.EncodeValue(types.NewMap(), nil)
	suite.serverCS.Put(c)
	suite.True(suite.serverCS.Commit(c.Hash(), hash.Hash{})) // change happens behind our backs
	suite.Equal(hash.Hash{}, suite.http.Root())              // shouldn't be visible yet

	suite.http.Rebase()
	suite.Equal(c.Hash(), suite.serverCS.Root())
}

func (suite *HTTPChunkStoreSuite) TestRoot() {
	c := types.EncodeValue(types.NewMap(), nil)
	suite.serverCS.Put(c)
	suite.True(suite.http.Commit(c.Hash(), hash.Hash{}))
	suite.Equal(c.Hash(), suite.serverCS.Root())
}

func (suite *HTTPChunkStoreSuite) TestVersionMismatch() {
	store := newBadVersionHTTPChunkStoreForTest(suite.serverCS)
	defer store.Close()
	c := types.EncodeValue(types.NewMap(), nil)
	suite.serverCS.Put(c)
	suite.Panics(func() { store.Commit(c.Hash(), hash.Hash{}) })
}

func (suite *HTTPChunkStoreSuite) TestCommit() {
	c := types.EncodeValue(types.NewMap(), nil)
	suite.serverCS.Put(c)
	suite.True(suite.http.Commit(c.Hash(), hash.Hash{}))
	suite.Equal(c.Hash(), suite.serverCS.Root())
}

func (suite *HTTPChunkStoreSuite) TestCommitWithParams() {
	u := fmt.Sprintf("http://localhost:9000?access_token=%s&other=19", testAuthToken)
	store := newAuthenticatingHTTPChunkStoreForTest(suite.Assert(), suite.serverCS, u)
	defer store.Close()
	c := types.EncodeValue(types.NewMap(), nil)
	suite.serverCS.Put(c)
	suite.True(store.Commit(c.Hash(), hash.Hash{}))
	suite.Equal(c.Hash(), suite.serverCS.Root())
}

func (suite *HTTPChunkStoreSuite) TestGet() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	suite.serverCS.PutMany(chnx)
	got := suite.http.Get(chnx[0].Hash())
	suite.Equal(chnx[0].Hash(), got.Hash())
	got = suite.http.Get(chnx[1].Hash())
	suite.Equal(chnx[1].Hash(), got.Hash())
}

func (suite *HTTPChunkStoreSuite) TestGetMany() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	notPresent := chunks.NewChunk([]byte("ghi")).Hash()
	suite.serverCS.PutMany(chnx)
	suite.serverCS.Flush()

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash(), notPresent)
	foundChunks := make(chan *chunks.Chunk)
	go func() { suite.http.GetMany(hashes, foundChunks); close(foundChunks) }()

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
	suite.http.PutMany(chnx)

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash())
	foundChunks := make(chan *chunks.Chunk)
	go func() { suite.http.GetMany(hashes, foundChunks); close(foundChunks) }()

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
	suite.serverCS.PutMany(chnx)
	suite.serverCS.Flush()
	suite.http.Put(cached)

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash(), cached.Hash())
	foundChunks := make(chan *chunks.Chunk)
	go func() { suite.http.GetMany(hashes, foundChunks); close(foundChunks) }()

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
	suite.serverCS.PutMany(chnx)
	got := suite.http.Get(chnx[0].Hash())
	suite.Equal(chnx[0].Hash(), got.Hash())
	got = suite.http.Get(chnx[1].Hash())
	suite.Equal(chnx[1].Hash(), got.Hash())
}

func (suite *HTTPChunkStoreSuite) TestHas() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	suite.serverCS.PutMany(chnx)
	suite.True(suite.http.Has(chnx[0].Hash()))
	suite.True(suite.http.Has(chnx[1].Hash()))
}

func (suite *HTTPChunkStoreSuite) TestHasMany() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	suite.serverCS.PutMany(chnx)
	suite.serverCS.Flush()
	notPresent := chunks.NewChunk([]byte("ghi")).Hash()

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash(), notPresent)
	present := suite.http.HasMany(hashes)

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
	suite.http.PutMany(chnx)
	suite.serverCS.Flush()

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash())
	present := suite.http.HasMany(hashes)

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
	suite.serverCS.PutMany(chnx)
	suite.serverCS.Flush()
	suite.http.Put(cached)

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash(), cached.Hash())
	present := suite.http.HasMany(hashes)

	suite.Len(present, len(chnx)+1)
	for _, c := range chnx {
		suite.True(present.Has(c.Hash()), "%s not present in %v", c.Hash(), present)
	}
}
