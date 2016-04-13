package datas

import (
	"net/http"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/constants"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/julienschmidt/httprouter"
	"github.com/stretchr/testify/suite"
)

func TestHTTPHintedChunkStore(t *testing.T) {
	suite.Run(t, &HTTPHintedChunkStoreTestSuite{})
}

type HTTPHintedChunkStoreTestSuite struct {
	suite.Suite
	cs    *chunks.TestStore
	store hintedChunkStore
}

type inlineServer struct {
	*httprouter.Router
}

func (serv inlineServer) Do(req *http.Request) (resp *http.Response, err error) {
	w := newFakeHTTPResponseWriter()
	serv.ServeHTTP(w, req)
	return w.resp, nil
}

func (suite *HTTPHintedChunkStoreTestSuite) SetupTest() {
	suite.cs = chunks.NewTestStore()
	suite.store = newHTTPHintedChunkStoreForTest(suite.cs)
}

func newHTTPHintedChunkStoreForTest(cs chunks.ChunkStore) hintedChunkStore {
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
	hcs := newHTTPHintedChunkStore("http://localhost:9000", "")
	hcs.httpClient = serv
	return hcs
}

func (suite *HTTPHintedChunkStoreTestSuite) TearDownTest() {
	suite.store.Close()
	suite.cs.Close()
}

func (suite *HTTPHintedChunkStoreTestSuite) TestPutChunk() {
	c := types.EncodeValue(types.NewString("abc"), nil)
	suite.store.Put(c, map[ref.Ref]struct{}{})
	suite.store.Flush()

	suite.Equal(1, suite.cs.Writes)
}

func (suite *HTTPHintedChunkStoreTestSuite) TestPutChunksInOrder() {
	chnx := []chunks.Chunk{
		types.EncodeValue(types.NewString("abc"), nil),
		types.EncodeValue(types.NewString("def"), nil),
	}
	l := types.NewList()
	for _, c := range chnx {
		suite.store.Put(c, map[ref.Ref]struct{}{})
		l = l.Append(types.NewRef(c.Ref()))
	}
	suite.store.Put(types.EncodeValue(l, nil), map[ref.Ref]struct{}{})
	suite.store.Flush()

	suite.Equal(3, suite.cs.Writes)
}

func (suite *HTTPHintedChunkStoreTestSuite) TestPutChunkWithHints() {
	chnx := []chunks.Chunk{
		types.EncodeValue(types.NewString("abc"), nil),
		types.EncodeValue(types.NewString("def"), nil),
	}
	suite.cs.PutMany(chnx)
	l := types.NewList(types.NewRef(chnx[0].Ref()), types.NewRef(chnx[1].Ref()))

	suite.store.Put(types.EncodeValue(l, nil), map[ref.Ref]struct{}{
		chnx[0].Ref(): struct{}{},
		chnx[1].Ref(): struct{}{},
	})
	suite.store.Flush()

	suite.Equal(3, suite.cs.Writes)
}

func (suite *HTTPHintedChunkStoreTestSuite) TestRoot() {
	c := chunks.NewChunk([]byte("abc"))
	suite.True(suite.cs.UpdateRoot(c.Ref(), ref.Ref{}))
	suite.Equal(c.Ref(), suite.store.Root())
}

func (suite *HTTPHintedChunkStoreTestSuite) TestUpdateRoot() {
	c := chunks.NewChunk([]byte("abc"))
	suite.True(suite.store.UpdateRoot(c.Ref(), ref.Ref{}))
	suite.Equal(c.Ref(), suite.cs.Root())
}

func (suite *HTTPHintedChunkStoreTestSuite) TestGet() {
	c := chunks.NewChunk([]byte("abc"))
	suite.cs.Put(c)
	got := suite.store.Get(c.Ref())
	suite.Equal(c.Ref(), got.Ref())
}
