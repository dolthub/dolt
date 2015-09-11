package chunks

import (
	"net/http"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
)

func TestHttpStoreTestSuite(t *testing.T) {
	suite.Run(t, &HttpStoreTestSuite{})
}

type HttpStoreTestSuite struct {
	ChunkStoreTestSuite
	server *httpStoreServer
}

func (suite *HttpStoreTestSuite) SetupTest() {
	suite.store = NewHttpStoreClient("http://localhost:8000")
	suite.server = NewHttpStoreServer(&MemoryStore{}, 8000)
	go suite.server.Run()

	// This call to a non-existing URL allows us to exit being sure that the server started. Otherwise, we sometimes get races with Stop() below.
	req, err := http.NewRequest("GET", "http://localhost:8000/notHere", nil)
	suite.NoError(err)
	http.DefaultClient.Do(req)
}

func (suite *HttpStoreTestSuite) TearDownTest() {
	suite.store.Close()
	suite.server.Stop()

	// Stop may have closed its side of an existing KeepAlive socket. In that case, the next call will clear the pending failure.
	req, err := http.NewRequest("GET", "http://localhost:8000/notHere", nil)
	suite.NoError(err)
	http.DefaultClient.Do(req)
}
