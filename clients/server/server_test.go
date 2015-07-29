package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
)

var datasetID = "testdataset"

func createTestStore() chunks.ChunkStore {
	ms := &chunks.MemoryStore{}
	datasetDs := dataset.NewDataset(datas.NewDataStore(ms), datasetID)
	datasetValue := types.NewString("Value for " + datasetID)
	datasetDs = datasetDs.Commit(datas.NewSetOfCommit().Insert(
		datas.NewCommit().SetParents(
			types.NewSet()).SetValue(datasetValue)))
	return ms
}

func TestBadRequest(t *testing.T) {
	assert := assert.New(t)

	req, _ := http.NewRequest("GET", "/bad", nil)
	w := httptest.NewRecorder()

	ms := &chunks.MemoryStore{}
	s := server{ms}
	s.handle(w, req)
	assert.Equal(w.Code, http.StatusBadRequest)
}

func TestRoot(t *testing.T) {
	assert := assert.New(t)

	req, _ := http.NewRequest("GET", "/root", nil)
	w := httptest.NewRecorder()
	ms := createTestStore()
	s := server{ms}
	s.handle(w, req)
	assert.Equal(w.Code, http.StatusOK)
	ref, err := ref.Parse(w.Body.String())
	assert.NoError(err)
	assert.Equal(ms.Root(), ref)
}

func TestGetRef(t *testing.T) {
	assert := assert.New(t)

	ms := createTestStore()
	rootRef := ms.Root().String()

	req, _ := http.NewRequest("GET", "/get?ref="+rootRef, nil)
	w := httptest.NewRecorder()
	s := server{ms}
	s.handle(w, req)
	assert.Equal(w.Code, http.StatusOK)
	assert.Equal(`j {"set":[{"ref":"sha1-be9c74bd5f8dcc6ad645fe729c1839c19ccaaeeb"}]}
`, w.Body.String())
}

func TestGetInvalidRef(t *testing.T) {
	assert := assert.New(t)

	ms := createTestStore()
	rootRef := "sha1-xxx"

	req, _ := http.NewRequest("GET", "/get?ref="+rootRef, nil)
	w := httptest.NewRecorder()
	s := server{ms}
	s.handle(w, req)
	assert.Equal(w.Code, http.StatusBadRequest)
}

func TestGetNonExistingRef(t *testing.T) {
	assert := assert.New(t)

	ms := createTestStore()
	ref := "sha1-1111111111111111111111111111111111111111"

	req, _ := http.NewRequest("GET", "/get?ref="+ref, nil)
	w := httptest.NewRecorder()
	s := server{ms}
	s.handle(w, req)
	assert.Equal(w.Code, http.StatusNotFound)
}

func TestGetDataset(t *testing.T) {
	assert := assert.New(t)

	ms := createTestStore()

	req, _ := http.NewRequest("GET", "/dataset?id="+datasetID, nil)
	w := httptest.NewRecorder()
	s := server{ms}
	s.handle(w, req)
	assert.Equal(w.Code, http.StatusOK)
}

func TestGetDatasetMissingParam(t *testing.T) {
	assert := assert.New(t)

	ms := createTestStore()

	req, _ := http.NewRequest("GET", "/dataset", nil)
	w := httptest.NewRecorder()
	s := server{ms}
	s.handle(w, req)
	assert.Equal(w.Code, http.StatusBadRequest)
}

func TestGetDatasetNotFound(t *testing.T) {
	assert := assert.New(t)

	ms := createTestStore()

	req, _ := http.NewRequest("GET", "/dataset?id=notfound", nil)
	w := httptest.NewRecorder()
	s := server{ms}
	s.handle(w, req)
	assert.Equal(w.Code, http.StatusNotFound)
}
