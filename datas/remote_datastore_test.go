package datas

import (
	"net/http"
	"os"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/stretchr/testify/suite"
)

func TestHTTPStoreTestSuite(t *testing.T) {
	if os.Getenv("RUN_HTTP_STORE_TEST") == "" {
		t.Skip("Skipping flaky HttpStoreTestSuite; to enable set RUN_HTTP_STORE_TEST env var.")
	}
	suite.Run(t, &HTTPStoreTestSuite{})
}

type HTTPStoreTestSuite struct {
	chunks.ChunkStoreTestSuite
	server *dataStoreServer
}

func (suite *HTTPStoreTestSuite) SetupTest() {
	suite.Store = chunks.NewHTTPStore("http://localhost:8000")
	suite.server = NewDataStoreServer(&localFactory{chunks.NewTestStoreFactory()}, 8000)
	go suite.server.Run()

	// This call to a non-existing URL allows us to exit being sure that the server started. Otherwise, we sometimes get races with Stop() below.
	req, err := http.NewRequest("GET", "http://localhost:8000/notHere", nil)
	suite.NoError(err)
	http.DefaultClient.Do(req)
}

func (suite *HTTPStoreTestSuite) TearDownTest() {
	suite.Store.Close()
	suite.server.Stop()

	// Stop may have closed its side of an existing KeepAlive socket. In that case, the next call will clear the pending failure.
	req, err := http.NewRequest("GET", "http://localhost:8000/notHere", nil)
	suite.NoError(err)
	http.DefaultClient.Do(req)
}

func TestNamespacedHTTPStoreTestSuite(t *testing.T) {
	if os.Getenv("RUN_HTTP_STORE_TEST") == "" {
		t.Skip("Skipping flaky HttpStoreTestSuite; to enable set RUN_HTTP_STORE_TEST env var.")
	}
	suite.Run(t, &NamespacedHTTPStoreTestSuite{})
}

type NamespacedHTTPStoreTestSuite struct {
	chunks.ChunkStoreTestSuite
	server  *dataStoreServer
	baseURL string
}

func (suite *NamespacedHTTPStoreTestSuite) SetupTest() {
	suite.baseURL = "http://localhost:8000/someStore"
	suite.Store = chunks.NewHTTPStore(suite.baseURL)
	suite.server = NewDataStoreServer(&localFactory{chunks.NewTestStoreFactory()}, 8000)
	go suite.server.Run()

	// This call to a non-existing URL allows us to exit being sure that the server started. Otherwise, we sometimes get races with Stop() below.
	req, err := http.NewRequest("GET", suite.baseURL+"/notHere", nil)
	suite.NoError(err)
	http.DefaultClient.Do(req)
}

func (suite *NamespacedHTTPStoreTestSuite) TearDownTest() {
	suite.Store.Close()
	suite.server.Stop()

	// Stop may have closed its side of an existing KeepAlive socket. In that case, the next call will clear the pending failure.
	req, err := http.NewRequest("GET", suite.baseURL+"/notHere", nil)
	suite.NoError(err)
	http.DefaultClient.Do(req)
}
