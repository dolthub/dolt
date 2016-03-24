package datas

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/constants"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/julienschmidt/httprouter"
	"github.com/stretchr/testify/suite"
)

func TestNotAHintedChunkSink(t *testing.T) {
	suite.Run(t, &NotAHintedChunkSinkTest{})
}

type NotAHintedChunkSinkTest struct {
	suite.Suite
	cs    *chunks.TestStore
	store hintedChunkSink
}

func (suite *NotAHintedChunkSinkTest) SetupTest() {
	suite.cs = chunks.NewTestStore()
	suite.store = newNotAHintedChunkStoreForTest(suite.cs)
}

func newNotAHintedChunkStoreForTest(cs chunks.ChunkStore) hintedChunkSink {
	serv := inlineServer{httprouter.New()}
	serv.POST(
		constants.PostRefsPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			HandlePostRefs(w, req, ps, cs)
		},
	)
	u, _ := url.Parse("http://localhost:9000")
	hcs := newNotAHintedChunkStore(u, "")
	hcs.httpClient = serv
	return hcs
}

func (suite *NotAHintedChunkSinkTest) TearDownTest() {
	suite.store.Close()
	suite.cs.Close()
}

func (suite *NotAHintedChunkSinkTest) TestPutChunks() {
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
