package datas

import (
	"compress/gzip"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"

	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/constants"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// DataStore provides versioned storage for noms values. Each DataStore instance represents one moment in history. Heads() returns the Commit from each active fork at that moment. The Commit() method returns a new DataStore, representing a new moment in history.
type RemoteDataStore struct {
	dataStoreCommon
}

func newRemoteDataStore(cs chunks.ChunkStore) *RemoteDataStore {
	return &RemoteDataStore{dataStoreCommon{cs, cs.Root(), nil, map[ref.Ref]bool{}, &sync.Mutex{}}}
}

func (rds *RemoteDataStore) host() *url.URL {
	return rds.dataStoreCommon.ChunkStore.(*chunks.HTTPStore).Host()
}

func (rds *RemoteDataStore) Commit(datasetID string, commit Commit) (DataStore, error) {
	err := rds.commit(datasetID, commit)
	return newRemoteDataStore(rds.ChunkStore), err
}

// Asks remote server to figure out which chunks need to be copied and return them.
func (rds *RemoteDataStore) CopyReachableChunksP(r, exclude ref.Ref, cs chunks.ChunkSink, concurrency int) {
	// POST http://<host>/ref/sha1----?all=true&exclude=sha1----. Response will be chunk data if present, 404 if absent.
	u := rds.host()
	u.Path = path.Join(constants.RefPath, r.String())

	values := &url.Values{}
	values.Add("all", "true")
	if !exclude.IsEmpty() {
		values.Add("exclude", exclude.String())
	}
	u.RawQuery = values.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	req.Header.Add("Accept-Encoding", "gzip")
	d.Chk.NoError(err)

	res, err := http.DefaultClient.Do(req)
	d.Chk.NoError(err)
	defer closeResponse(res)
	d.Chk.Equal(http.StatusOK, res.StatusCode, "Unexpected response: %s", http.StatusText(res.StatusCode))

	reader := res.Body
	if strings.Contains(res.Header.Get("Content-Encoding"), "gzip") {
		gr, err := gzip.NewReader(reader)
		d.Chk.NoError(err)
		defer gr.Close()
		reader = gr
	}

	chunks.Deserialize(reader, cs, nil)
}

// In order for keep alive to work we must read to EOF on every response. We may want to add a timeout so that a server that left its connection open can't cause all of ports to be eaten up.
func closeResponse(res *http.Response) error {
	data, err := ioutil.ReadAll(res.Body)
	d.Chk.NoError(err)
	d.Chk.Equal(0, len(data))
	return res.Body.Close()
}
