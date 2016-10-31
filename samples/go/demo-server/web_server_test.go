// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/receipts"
	"github.com/attic-labs/testify/assert"
)

func TestRoot(t *testing.T) {
	assert := assert.New(t)

	factory := chunks.NewMemoryStoreFactory()
	defer factory.Shutter()

	router = setupWebServer(factory)
	defer func() { router = nil }()

	dbName := "/test/db"

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

	// Auth with master key:
	authKey = "goodAuthKey"
	wrongKey := "wrongAuthKey"

	testWriteValue(t, "/test/db", authKey, true, true)
	testWriteValue(t, "/test/db", wrongKey, true, false)
	testWriteValue(t, "/p/test/db", authKey, true, true)
	testWriteValue(t, "/p/test/db", wrongKey, false, false)

	// Auth with receipt encrypted with empty (invalid) key:
	receipt, err := receipts.Generate(receiptKey, receipts.Data{
		Database:  "/p/test/db",
		IssueDate: time.Now(),
	})
	assert.NoError(err)

	testWriteValue(t, "/p/test/db", receipt, false, false)
	testWriteValue(t, "/p/test/db2", receipt, false, false)

	// Auth with good receipt:
	rand.Read(receiptKey[:])

	receipt, err = receipts.Generate(receiptKey, receipts.Data{
		Database:  "/p/test/db",
		IssueDate: time.Now(),
	})
	assert.NoError(err)

	testWriteValue(t, "/p/test/db", receipt, true, true)
	testWriteValue(t, "/p/test/db2", receipt, false, false)

	// Auth with wrong receipt (different receipt key):
	var wrongReceiptKey receipts.Key
	rand.Read(wrongReceiptKey[:])

	receipt, err = receipts.Generate(wrongReceiptKey, receipts.Data{
		Database:  "/p/test/db",
		IssueDate: time.Now(),
	})
	assert.NoError(err)

	testWriteValue(t, "/p/test/db", receipt, false, false)
	testWriteValue(t, "/p/test/db2", receipt, false, false)

	// Receipts cannot grant write access to non-private databases:
	receipt, err = receipts.Generate(receiptKey, receipts.Data{
		Database:  "/test/db",
		IssueDate: time.Now(),
	})
	assert.NoError(err)

	testWriteValue(t, "/test/db", receipt, true, false)
	testWriteValue(t, "/test/db2", receipt, true, false)
}

func testWriteValue(t *testing.T, dbName, testAuthKey string, expectRead, expectWrite bool) {
	assert := assert.New(t)
	factory := chunks.NewMemoryStoreFactory()
	defer factory.Shutter()

	router = setupWebServer(factory)
	defer func() { router = nil }()

	testString := "Now, what?"

	var (
		w        *httptest.ResponseRecorder
		r        *http.Request
		err      error
		lastRoot *bytes.Buffer
	)

	// GET /root/

	runTestGetRoot := func(key string) {
		path := dbName + constants.RootPath + prefixIfNotEmpty("?access_token=", key)
		r, err = newRequest("GET", path, nil)
		assert.NoError(err)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, r)
		lastRoot = w.Body
	}

	runTestGetRoot(testAuthKey)

	if expectRead {
		assert.Equal(http.StatusOK, w.Code)
	} else {
		assert.Equal(http.StatusUnauthorized, w.Code)
		runTestGetRoot(authKey) // this should always succeed
	}

	// POST /writeValue/ preamble

	craftCommit := func(v types.Value) types.Struct {
		return datas.NewCommit(v, types.NewSet(), types.NewStruct("Meta", types.StructData{}))
	}

	tval := craftCommit(types.Bool(true))
	wval := craftCommit(types.String(testString))
	chunk1 := types.EncodeValue(tval, nil)
	chunk2 := types.EncodeValue(wval, nil)
	refMap := types.NewMap(
		types.String("ds1"), types.NewRef(tval),
		types.String("ds2"), types.NewRef(wval))
	chunk3 := types.EncodeValue(refMap, nil)

	body := &bytes.Buffer{}
	// we would use this func, but it's private so use next line instead: serializeHints(body, map[ref.Ref]struct{}{hint: struct{}{}})
	err = binary.Write(body, binary.BigEndian, uint32(0))
	assert.NoError(err)

	chunks.Serialize(chunk1, body)
	chunks.Serialize(chunk2, body)
	chunks.Serialize(chunk3, body)

	// POST /writeValue/

	runTestPostWriteValue := func(key string) {
		path := dbName + constants.WriteValuePath + prefixIfNotEmpty("?access_token=", key)
		w = httptest.NewRecorder()
		r, err = newRequest("POST", path, ioutil.NopCloser(body))
		assert.NoError(err)
		router.ServeHTTP(w, r)
	}

	runTestPostWriteValue(testAuthKey)

	if expectWrite {
		assert.Equal(http.StatusCreated, w.Code)
	} else {
		assert.Equal(http.StatusUnauthorized, w.Code)
		runTestPostWriteValue(authKey) // this should always succeed
	}

	// POST /root/

	runTestPostRoot := func(key string) {
		args := fmt.Sprintf("?last=%s&current=%s", lastRoot, types.NewRef(refMap).TargetHash())
		path := dbName + constants.RootPath + args + prefixIfNotEmpty("&access_token=", key)
		w = httptest.NewRecorder()
		r, _ = newRequest("POST", path, ioutil.NopCloser(body))
		router.ServeHTTP(w, r)
	}

	runTestPostRoot(testAuthKey)

	if expectWrite {
		assert.Equal(http.StatusOK, w.Code, string(w.Body.Bytes()))
	} else {
		assert.Equal(http.StatusUnauthorized, w.Code)
		runTestPostRoot(authKey) // this should always succeed
	}

	// POST /getRefs/

	whash := wval.Hash()
	hints := map[hash.Hash]struct{}{whash: {}}
	rdr := buildGetRefsRequestBody(hints)

	runTestPostGetRefs := func(key string) {
		path := dbName + constants.GetRefsPath + prefixIfNotEmpty("?access_token=", key)
		w = httptest.NewRecorder()
		r, _ = newRequest("POST", path, rdr)
		r.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		router.ServeHTTP(w, r)
	}

	runTestPostGetRefs(testAuthKey)

	if expectRead {
		assert.Equal(http.StatusOK, w.Code, string(w.Body.Bytes()))
	} else {
		assert.Equal(http.StatusUnauthorized, w.Code)
		runTestPostGetRefs(authKey) // this should always succeed
	}

	ms := chunks.NewMemoryStore()
	chunks.Deserialize(w.Body, ms, nil)
	v := types.DecodeValue(ms.Get(whash), datas.NewDatabase(ms))
	assert.Equal(testString, string(v.(types.Struct).Get(datas.ValueField).(types.String)))
}

func newRequest(method, url string, body io.Reader) (req *http.Request, err error) {
	req, err = http.NewRequest(method, url, body)
	if err != nil {
		return
	}
	req.Header.Set(datas.NomsVersionHeader, constants.NomsVersion)
	return
}

func prefixIfNotEmpty(prefix, s string) string {
	if s != "" {
		return prefix + s
	}
	return ""
}
