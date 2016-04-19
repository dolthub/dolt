package datas

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/constants"
	"github.com/attic-labs/noms/types"
	"github.com/julienschmidt/httprouter"
	"github.com/stretchr/testify/suite"
)

func TestNotABatchSink(t *testing.T) {
	suite.Run(t, &NotABatchSinkSuite{})
}

type NotABatchSinkSuite struct {
	suite.Suite
	cs    *chunks.TestStore
	store batchSink
}

func (suite *NotABatchSinkSuite) SetupTest() {
	suite.cs = chunks.NewTestStore()
	suite.store = newNotAHintedBatchSinkForTest(suite.cs)
}

func newNotAHintedBatchSinkForTest(cs chunks.ChunkStore) batchSink {
	serv := inlineServer{httprouter.New()}
	serv.POST(
		constants.PostRefsPath,
		func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
			HandlePostRefs(w, req, ps, cs)
		},
	)
	u, _ := url.Parse("http://localhost:9000")
	hcs := newNotABatchSink(u, "")
	hcs.httpClient = serv
	return hcs
}

func (suite *NotABatchSinkSuite) TearDownTest() {
	suite.store.Close()
	suite.cs.Close()
}

func (suite *NotABatchSinkSuite) TestPutChunks() {
	chnx := []chunks.Chunk{
		types.EncodeValue(types.NewString("abc"), nil),
		types.EncodeValue(types.NewString("def"), nil),
	}
	l := types.NewList()
	for _, c := range chnx {
		suite.store.SchedulePut(c, types.Hints{})
		l = l.Append(types.NewRef(c.Ref()))
	}
	suite.store.SchedulePut(types.EncodeValue(l, nil), types.Hints{})
	suite.store.Flush()

	suite.Equal(3, suite.cs.Writes)
}
