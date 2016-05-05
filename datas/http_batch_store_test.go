package datas

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/constants"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/julienschmidt/httprouter"
	"github.com/stretchr/testify/suite"
)

func TestHTTPBatchStore(t *testing.T) {
	suite.Run(t, &HTTPBatchStoreSuite{})
}

type HTTPBatchStoreSuite struct {
	suite.Suite
	cs    *chunks.TestStore
	store *httpBatchStore
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

func (suite *HTTPBatchStoreSuite) SetupTest() {
	suite.cs = chunks.NewTestStore()
	suite.store = newHTTPBatchStoreForTest(suite.cs)
}

func newHTTPBatchStoreForTest(cs chunks.ChunkStore) *httpBatchStore {
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
	hcs := newHTTPBatchStore("http://localhost:9000", "")
	hcs.httpClient = serv
	return hcs
}

func (suite *HTTPBatchStoreSuite) TearDownTest() {
	suite.store.Close()
	suite.cs.Close()
}

func (suite *HTTPBatchStoreSuite) TestPutChunk() {
	c := types.EncodeValue(types.NewString("abc"), nil)
	suite.store.SchedulePut(c, types.Hints{})
	suite.store.Flush()

	suite.Equal(1, suite.cs.Writes)
}

func (suite *HTTPBatchStoreSuite) TestPutChunksInOrder() {
	chnx := []chunks.Chunk{
		types.EncodeValue(types.NewString("abc"), nil),
		types.EncodeValue(types.NewString("def"), nil),
	}
	l := types.NewList()
	for _, c := range chnx {
		suite.store.SchedulePut(c, types.Hints{})
		l = l.Append(newStringRef(c.Ref(), 1))
	}
	suite.store.SchedulePut(types.EncodeValue(l, nil), types.Hints{})
	suite.store.Flush()

	suite.Equal(3, suite.cs.Writes)
}

func (suite *HTTPBatchStoreSuite) TestPutChunkWithHints() {
	chnx := []chunks.Chunk{
		types.EncodeValue(types.NewString("abc"), nil),
		types.EncodeValue(types.NewString("def"), nil),
	}
	suite.NoError(suite.cs.PutMany(chnx))
	l := types.NewList(newStringRef(chnx[0].Ref(), 1), newStringRef(chnx[1].Ref(), 1))

	suite.store.SchedulePut(types.EncodeValue(l, nil), types.Hints{
		chnx[0].Ref(): struct{}{},
		chnx[1].Ref(): struct{}{},
	})
	suite.store.Flush()

	suite.Equal(3, suite.cs.Writes)
}

type backpressureCS struct {
	chunks.ChunkStore
	tries int
}

func (b *backpressureCS) PutMany(chnx []chunks.Chunk) chunks.BackpressureError {
	if chnx == nil {
		return nil
	}
	b.tries++

	if len(chnx) <= b.tries {
		return b.ChunkStore.PutMany(chnx)
	}
	if bpe := b.ChunkStore.PutMany(chnx[:b.tries]); bpe != nil {
		return bpe
	}

	bpe := make(chunks.BackpressureError, len(chnx)-b.tries)
	for i, c := range chnx[b.tries:] {
		bpe[i] = c.Ref()
	}
	return bpe
}

func (suite *HTTPBatchStoreSuite) TestPutChunksBackpressure() {
	bpcs := &backpressureCS{ChunkStore: suite.cs}
	bs := newHTTPBatchStoreForTest(bpcs)
	defer bs.Close()
	defer bpcs.Close()

	chnx := []chunks.Chunk{
		types.EncodeValue(types.NewString("abc"), nil),
		types.EncodeValue(types.NewString("def"), nil),
	}
	l := types.NewList()
	for _, c := range chnx {
		bs.SchedulePut(c, types.Hints{})
		l = l.Append(newStringRef(c.Ref(), 1))
	}
	bs.SchedulePut(types.EncodeValue(l, nil), types.Hints{})
	bs.Flush()

	suite.Equal(6, suite.cs.Writes)
}

func (suite *HTTPBatchStoreSuite) TestRoot() {
	c := chunks.NewChunk([]byte("abc"))
	suite.True(suite.cs.UpdateRoot(c.Ref(), ref.Ref{}))
	suite.Equal(c.Ref(), suite.store.Root())
}

func (suite *HTTPBatchStoreSuite) TestUpdateRoot() {
	c := chunks.NewChunk([]byte("abc"))
	suite.True(suite.store.UpdateRoot(c.Ref(), ref.Ref{}))
	suite.Equal(c.Ref(), suite.cs.Root())
}

func (suite *HTTPBatchStoreSuite) TestGet() {
	c := chunks.NewChunk([]byte("abc"))
	suite.cs.Put(c)
	got := suite.store.Get(c.Ref())
	suite.Equal(c.Ref(), got.Ref())
}

func newStringRef(r ref.Ref, height uint64) types.Ref {
	return types.NewTypedRef(types.MakeRefType(types.StringType), r, height)
}
