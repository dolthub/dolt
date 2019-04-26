// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/julienschmidt/httprouter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
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
			cs.Rebase(context.Background())
			HandleWriteValue(w, req, ps, cs)
		},
	)
	serv.POST(
		constants.GetRefsPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase(context.Background())
			HandleGetRefs(w, req, ps, cs)
		},
	)
	serv.POST(
		constants.HasRefsPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase(context.Background())
			HandleHasRefs(w, req, ps, cs)
		},
	)
	serv.POST(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase(context.Background())
			HandleRootPost(w, req, ps, cs)
		},
	)
	serv.GET(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase(context.Background())
			HandleRootGet(w, req, ps, cs)
		},
	)
	serv.GET(
		constants.StatsPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase(context.Background())
			HandleStats(w, req, ps, cs)
		},
	)
	return newHTTPChunkStoreWithClient(context.Background(), "http://localhost:9000", "", serv)
}

func newAuthenticatingHTTPChunkStoreForTest(assert *assert.Assertions, cs chunks.ChunkStore, hostUrl string) *httpChunkStore {
	authenticate := func(req *http.Request) {
		assert.Equal(testAuthToken, req.URL.Query().Get("access_token"))
	}

	serv := inlineServer{httprouter.New()}
	serv.POST(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase(context.Background())
			authenticate(req)
			HandleRootPost(w, req, ps, cs)
		},
	)
	serv.GET(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase(context.Background())
			HandleRootGet(w, req, ps, cs)
		},
	)
	return newHTTPChunkStoreWithClient(context.Background(), hostUrl, "", serv)
}

func newBadVersionHTTPChunkStoreForTest(cs chunks.ChunkStore) *httpChunkStore {
	serv := inlineServer{httprouter.New()}
	serv.POST(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase(context.Background())
			HandleRootPost(w, req, ps, cs)
			w.Header().Set(NomsVersionHeader, "BAD")
		},
	)
	serv.GET(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			cs.Rebase(context.Background())
			HandleRootGet(w, req, ps, cs)
		},
	)
	return newHTTPChunkStoreWithClient(context.Background(), "http://localhost", "", serv)
}

func (suite *HTTPChunkStoreSuite) TearDownTest() {
	suite.http.Close()
	suite.serverCS.Close()
}

func (suite *HTTPChunkStoreSuite) TestPutChunk() {
	c := types.EncodeValue(types.String("abc"))
	suite.http.Put(context.Background(), c)
	suite.True(suite.http.Has(context.Background(), c.Hash()))

	suite.True(suite.http.Commit(context.Background(), hash.Hash{}, hash.Hash{}))
	suite.Equal(1, suite.serverCS.Writes)
}

func (suite *HTTPChunkStoreSuite) TestPutChunksInOrder() {
	vals := []types.Value{
		types.String("abc"),
		types.String("def"),
	}
	vs := types.NewValueStore(suite.serverCS)
	defer vs.Close()
	le := types.NewList(context.Background(), vs).Edit()
	for _, val := range vals {
		suite.http.Put(context.Background(), types.EncodeValue(val))
		le.Append(types.NewRef(val))
	}
	suite.http.Put(context.Background(), types.EncodeValue(le.List(context.Background())))
	suite.True(suite.http.Commit(context.Background(), hash.Hash{}, hash.Hash{}))

	suite.Equal(3, suite.serverCS.Writes)
}

func (suite *HTTPChunkStoreSuite) TestStats() {
	suite.http.Put(context.Background(), types.EncodeValue(types.String("abc")))
	suite.http.Put(context.Background(), types.EncodeValue(types.String("def")))

	suite.True(suite.http.Commit(context.Background(), hash.Hash{}, hash.Hash{}))

	suite.NotEmpty(suite.http.StatsSummary())
}

func (suite *HTTPChunkStoreSuite) TestRebase() {
	suite.Equal(hash.Hash{}, suite.http.Root(context.Background()))
	db := NewDatabase(suite.serverCS)
	defer db.Close()
	c := types.EncodeValue(types.NewMap(context.Background(), db))
	suite.serverCS.Put(context.Background(), c)
	suite.True(suite.serverCS.Commit(context.Background(), c.Hash(), hash.Hash{})) // change happens behind our backs
	suite.Equal(hash.Hash{}, suite.http.Root(context.Background()))                // shouldn't be visible yet

	suite.http.Rebase(context.Background())
	suite.Equal(c.Hash(), suite.serverCS.Root(context.Background()))
}

func (suite *HTTPChunkStoreSuite) TestRoot() {
	db := NewDatabase(suite.serverCS)
	defer db.Close()
	c := types.EncodeValue(types.NewMap(context.Background(), db))
	suite.serverCS.Put(context.Background(), c)
	suite.True(suite.http.Commit(context.Background(), c.Hash(), hash.Hash{}))
	suite.Equal(c.Hash(), suite.serverCS.Root(context.Background()))
}

func (suite *HTTPChunkStoreSuite) TestVersionMismatch() {
	store := newBadVersionHTTPChunkStoreForTest(suite.serverCS)
	vs := types.NewValueStore(store)
	defer vs.Close()
	c := types.EncodeValue(types.NewMap(context.Background(), vs))
	suite.serverCS.Put(context.Background(), c)
	suite.Panics(func() { store.Commit(context.Background(), c.Hash(), hash.Hash{}) })
}

func (suite *HTTPChunkStoreSuite) TestCommit() {
	db := NewDatabase(suite.serverCS)
	defer db.Close()
	c := types.EncodeValue(types.NewMap(context.Background(), db))
	suite.serverCS.Put(context.Background(), c)
	suite.True(suite.http.Commit(context.Background(), c.Hash(), hash.Hash{}))
	suite.Equal(c.Hash(), suite.serverCS.Root(context.Background()))
}

func (suite *HTTPChunkStoreSuite) TestEmptyHashCommit() {
	suite.True(suite.http.Commit(context.Background(), hash.Hash{}, hash.Hash{}))
	suite.Equal(hash.Hash{}, suite.serverCS.Root(context.Background()))
}

func (suite *HTTPChunkStoreSuite) TestCommitWithParams() {
	u := fmt.Sprintf("http://localhost:9000?access_token=%s&other=19", testAuthToken)
	store := newAuthenticatingHTTPChunkStoreForTest(suite.Assert(), suite.serverCS, u)
	vs := types.NewValueStore(store)
	defer vs.Close()
	c := types.EncodeValue(types.NewMap(context.Background(), vs))
	suite.serverCS.Put(context.Background(), c)
	suite.True(store.Commit(context.Background(), c.Hash(), hash.Hash{}))
	suite.Equal(c.Hash(), suite.serverCS.Root(context.Background()))
}

func (suite *HTTPChunkStoreSuite) TestGet() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	for _, c := range chnx {
		suite.serverCS.Put(context.Background(), c)
	}
	got := suite.http.Get(context.Background(), chnx[0].Hash())
	suite.Equal(chnx[0].Hash(), got.Hash())
	got = suite.http.Get(context.Background(), chnx[1].Hash())
	suite.Equal(chnx[1].Hash(), got.Hash())
}

func (suite *HTTPChunkStoreSuite) TestGetMany() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	notPresent := chunks.NewChunk([]byte("ghi")).Hash()
	for _, c := range chnx {
		suite.serverCS.Put(context.Background(), c)
	}
	persistChunks(context.Background(), suite.serverCS)

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash(), notPresent)
	foundChunks := make(chan *chunks.Chunk)
	go func() { suite.http.GetMany(context.Background(), hashes, foundChunks); close(foundChunks) }()

	for c := range foundChunks {
		hashes.Remove(c.Hash())
	}
	suite.Len(hashes, 1)
	suite.True(hashes.Has(notPresent))
}

func (suite *HTTPChunkStoreSuite) TestOverGetThreshold_Issue3589() {
	if testing.Short() {
		suite.T().Skip("Skipping test in short mode.")
	}
	// BUG 3589 happened because we requested enough hashes that the body was over 10MB. The new way of encoding getRefs request bodies means that 10MB will no longer be a limitation. This test will generate a request larger than 10MB.
	count := ((10 * (1 << 20)) / hash.ByteLen) + 1
	hashes := make(hash.HashSet, count)
	for i := 0; i < count-1; i++ {
		h := hash.Hash{}
		binary.BigEndian.PutUint64(h[hash.ByteLen-8:], uint64(i))
		hashes.Insert(h)
	}

	present := chunks.NewChunk([]byte("ghi"))
	suite.serverCS.Put(context.Background(), present)
	persistChunks(context.Background(), suite.serverCS)
	hashes.Insert(present.Hash())

	foundChunks := make(chan *chunks.Chunk)
	go func() { suite.http.GetMany(context.Background(), hashes, foundChunks); close(foundChunks) }()

	found := hash.HashSet{}
	for c := range foundChunks {
		found.Insert(c.Hash())
	}
	suite.Len(found, 1)
	suite.True(found.Has(present.Hash()))
}

func (suite *HTTPChunkStoreSuite) TestGetManyAllCached() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	for _, c := range chnx {
		suite.http.Put(context.Background(), c)
	}

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash())
	foundChunks := make(chan *chunks.Chunk)
	go func() { suite.http.GetMany(context.Background(), hashes, foundChunks); close(foundChunks) }()

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
	for _, c := range chnx {
		suite.serverCS.Put(context.Background(), c)
	}
	persistChunks(context.Background(), suite.serverCS)
	suite.http.Put(context.Background(), cached)

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash(), cached.Hash())
	foundChunks := make(chan *chunks.Chunk)
	go func() { suite.http.GetMany(context.Background(), hashes, foundChunks); close(foundChunks) }()

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
	for _, c := range chnx {
		suite.serverCS.Put(context.Background(), c)
	}
	got := suite.http.Get(context.Background(), chnx[0].Hash())
	suite.Equal(chnx[0].Hash(), got.Hash())
	got = suite.http.Get(context.Background(), chnx[1].Hash())
	suite.Equal(chnx[1].Hash(), got.Hash())
}

func (suite *HTTPChunkStoreSuite) TestGetWithRoot() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	for _, c := range chnx {
		suite.serverCS.Put(context.Background(), c)
	}
	suite.serverCS.Commit(context.Background(), chnx[0].Hash(), hash.Hash{})

	serv := inlineServer{httprouter.New()}
	serv.GET(
		constants.RootPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			suite.serverCS.Rebase(context.Background())
			HandleRootGet(w, req, ps, suite.serverCS)
		},
	)
	serv.POST(
		constants.GetRefsPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			r := req.URL.Query().Get("root")
			suite.Equal(chnx[0].Hash().String(), r)
			suite.serverCS.Rebase(context.Background())
			HandleGetRefs(w, req, ps, suite.serverCS)
		},
	)
	store := newHTTPChunkStoreWithClient(context.Background(), "http://localhost:9000", "", serv)

	got := store.Get(context.Background(), chnx[1].Hash())
	suite.Equal(chnx[1].Hash(), got.Hash())
}

func (suite *HTTPChunkStoreSuite) TestHas() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	for _, c := range chnx {
		suite.serverCS.Put(context.Background(), c)
	}
	suite.True(suite.http.Has(context.Background(), chnx[0].Hash()))
	suite.True(suite.http.Has(context.Background(), chnx[1].Hash()))
}

func (suite *HTTPChunkStoreSuite) TestHasMany() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	for _, c := range chnx {
		suite.serverCS.Put(context.Background(), c)
	}
	persistChunks(context.Background(), suite.serverCS)
	notPresent := chunks.NewChunk([]byte("ghi")).Hash()

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash(), notPresent)
	absent := suite.http.HasMany(context.Background(), hashes)

	suite.Len(absent, 1)
	for _, c := range chnx {
		suite.False(absent.Has(c.Hash()), "%s present in %v", c.Hash(), absent)
	}
	suite.True(absent.Has(notPresent))
}

func (suite *HTTPChunkStoreSuite) TestHasManyAllCached() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	for _, c := range chnx {
		suite.http.Put(context.Background(), c)
	}
	persistChunks(context.Background(), suite.serverCS)

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash())
	absent := suite.http.HasMany(context.Background(), hashes)

	suite.Len(absent, 0)
	for _, c := range chnx {
		suite.False(absent.Has(c.Hash()), "%s present in %v", c.Hash(), absent)
	}
}

func (suite *HTTPChunkStoreSuite) TestHasManySomeCached() {
	chnx := []chunks.Chunk{
		chunks.NewChunk([]byte("abc")),
		chunks.NewChunk([]byte("def")),
	}
	cached := chunks.NewChunk([]byte("ghi"))
	for _, c := range chnx {
		suite.serverCS.Put(context.Background(), c)
	}
	persistChunks(context.Background(), suite.serverCS)
	suite.http.Put(context.Background(), cached)

	hashes := hash.NewHashSet(chnx[0].Hash(), chnx[1].Hash(), cached.Hash())
	absent := suite.http.HasMany(context.Background(), hashes)

	suite.Len(absent, 0)
	for _, c := range chnx {
		suite.False(absent.Has(c.Hash()), "%s present in %v", c.Hash(), absent)
	}
	suite.False(absent.Has(cached.Hash()), "%s present in %v", cached.Hash(), absent)
}
