// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

var (
	dbName = "/test/db"
)

func TestRoot(t *testing.T) {
	assert := assert.New(t)

	factory := chunks.NewMemoryStoreFactory()
	defer factory.Shutter()

	router = setupWebServer(factory)
	defer func() { router = nil }()

	w := httptest.NewRecorder()
	r, _ := newRequest("GET", dbName+constants.RootPath, nil)
	router.ServeHTTP(w, r)
	assert.Equal("00000000000000000000000000000000", w.Body.String())

	w = httptest.NewRecorder()
	r, _ = newRequest("OPTIONS", dbName+constants.RootPath, nil)
	r.Header.Add("Origin", "http://www.noms.io")
	router.ServeHTTP(w, r)
	assert.Equal(w.HeaderMap["Access-Control-Allow-Origin"][0], "http://www.noms.io")
}

func buildGetRefsRequestBody(hashes map[hash.Hash]struct{}) io.Reader {
	values := &url.Values{}
	for h := range hashes {
		values.Add("ref", h.String())
	}
	return strings.NewReader(values.Encode())
}

func TestWriteValue(t *testing.T) {
	assert := assert.New(t)
	factory := chunks.NewMemoryStoreFactory()
	defer factory.Shutter()

	router = setupWebServer(factory)
	defer func() { router = nil }()

	testString := "Now, what?"
	authKey = "anauthkeyvalue"

	w := httptest.NewRecorder()
	r, err := newRequest("GET", dbName+constants.RootPath, nil)
	assert.NoError(err)
	router.ServeHTTP(w, r)
	lastRoot := w.Body
	assert.Equal(http.StatusOK, w.Code)

	tval := types.Bool(true)
	wval := types.String(testString)
	chunk1 := types.EncodeValue(tval, nil)
	chunk2 := types.EncodeValue(wval, nil)
	refList := types.NewList(types.NewRef(tval), types.NewRef(wval))
	chunk3 := types.EncodeValue(refList, nil)

	body := &bytes.Buffer{}
	// we would use this func, but it's private so use next line instead: serializeHints(body, map[ref.Ref]struct{}{hint: struct{}{}})
	err = binary.Write(body, binary.BigEndian, uint32(0))
	assert.NoError(err)

	sz := chunks.NewSerializer(body)
	sz.Put(chunk1)
	sz.Put(chunk2)
	sz.Put(chunk3)
	sz.Close()

	w = httptest.NewRecorder()
	r, err = newRequest("POST", dbName+constants.WriteValuePath+"?access_token="+authKey, ioutil.NopCloser(body))
	assert.NoError(err)
	router.ServeHTTP(w, r)
	assert.Equal(http.StatusCreated, w.Code)

	w = httptest.NewRecorder()
	args := fmt.Sprintf("&last=%s&current=%s", lastRoot, types.NewRef(refList).TargetHash())
	r, _ = newRequest("POST", dbName+constants.RootPath+"?access_token="+authKey+args, ioutil.NopCloser(body))
	router.ServeHTTP(w, r)
	assert.Equal(http.StatusOK, w.Code)

	whash := wval.Hash()
	hints := map[hash.Hash]struct{}{whash: struct{}{}}
	rdr := buildGetRefsRequestBody(hints)
	r, _ = newRequest("POST", dbName+constants.GetRefsPath, rdr)
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	router.ServeHTTP(w, r)
	assert.Equal(http.StatusOK, w.Code)

	ms := chunks.NewMemoryStore()
	chunks.Deserialize(w.Body, ms, nil)
	v := types.DecodeValue(ms.Get(whash), datas.NewDatabase(ms))
	assert.Equal(testString, string(v.(types.String)))
}

func newRequest(method, url string, body io.Reader) (req *http.Request, err error) {
	req, err = http.NewRequest(method, url, body)
	if err != nil {
		return
	}
	req.Header.Set(datas.NomsVersionHeader, constants.NomsVersion)
	return
}
