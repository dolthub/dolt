// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/constants"
	"github.com/attic-labs/noms/types"
	"github.com/julienschmidt/httprouter"
	"github.com/attic-labs/testify/suite"
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
	vals := []types.Value{
		types.NewString("abc"),
		types.NewString("def"),
	}
	l := types.NewList()
	for _, v := range vals {
		suite.store.SchedulePut(types.EncodeValue(v, nil), 1, types.Hints{})
		l = l.Append(types.NewRef(v))
	}
	suite.store.SchedulePut(types.EncodeValue(l, nil), 2, types.Hints{})
	suite.store.Flush()

	suite.Equal(3, suite.cs.Writes)
}
