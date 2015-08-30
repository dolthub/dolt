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
}

func (suite *HttpStoreTestSuite) TearDownTest() {
	suite.server.Stop()

	// Stop will have closed it's side of an existing KeepAlive socket. The next request will fail.
	req, err := http.NewRequest("GET", "http://localhost:8000", nil)
	suite.NoError(err)
	_, err = http.DefaultClient.Do(req)
	suite.Error(err)
}
