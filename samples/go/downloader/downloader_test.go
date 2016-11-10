// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"
)

func TestDownloader(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	clienttest.ClientTestSuite
}

func (s testSuite) TestMain() {
	commitToDb := func(v types.Value, dsName string, dbSpec string) {
		sp, err := spec.ForDataset(dbSpec + spec.Separator + dsName)
		s.NoError(err)
		defer sp.Close()
		sp.GetDatabase().Commit(sp.GetDataset(), v, datas.CommitOptions{})
	}

	testBlobValue := func(db datas.Database, m types.Map, key, expected string) {
		k := types.String(key)
		localResource1 := m.Get(k).(types.Struct)
		blob1 := localResource1.Get("blobRef").(types.Ref).TargetValue(db).(types.Blob)
		s.Equal("BlobText, url: "+expected, stringFromBlob(blob1))
	}

	mustRunTest := func(args []string, expected string) {
		stdout, _ := s.MustRun(main, args)
		s.Contains(lastLine(stdout), expected)
	}

	errorTest := func(args []string, expected string) {
		_, stderr, recoveredErr := s.Run(main, args)
		exitError, ok := recoveredErr.(clienttest.ExitError)
		if s.True(ok) {
			s.Equal(exitError.Code, 1)
		}
		s.Contains(stderr, expected)
	}

	dbSpecString := spec.CreateDatabaseSpecString("ldb", s.LdbDir)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "BlobText, url: "+r.URL.Path)
	}))
	defer ts.Close()

	m := map[string]RemoteResource{
		"k1": RemoteResource{ts.URL + "/one"},
	}
	commitToDb(mustMarshal(m), "in-ds", dbSpecString)
	mustRunTest(
		[]string{"--cache-ds", "cache", dbSpecString + "::in-ds.value", "out-ds"},
		"walked: 3, updated 1, found in cache: 0, errors retrieving: 0",
	)

	m["k2"] = RemoteResource{ts.URL + "/two"}
	commitToDb(mustMarshal(m), "in-ds", dbSpecString)

	mustRunTest(
		[]string{"--cache-ds", "cache", dbSpecString + "::in-ds.value", "out-ds"},
		"walked: 1, updated 1, found in cache: 0, errors retrieving: 0",
	)

	sp, err := spec.ForPath(dbSpecString + "::out-ds.value")
	s.NoError(err)
	defer sp.Close()

	db, v := sp.GetDatabase(), sp.GetValue().(types.Map)
	testBlobValue(db, v, "k1", "/one")
	testBlobValue(db, v, "k2", "/two")

	mustRunTest(
		[]string{"--cache-ds", "cache", dbSpecString + "::in-ds.value", "out-ds"},
		"No change since last run, doing nothing",
	)

	errorTest(
		[]string{"--cache-ds", "cache", "--concurrency", "0", dbSpecString + "::in-ds.value", "out-ds"},
		"concurrency cannot be less than 1",
	)

	errorTest(
		[]string{dbSpecString + "::in-ds.value"},
		"missing required argument",
	)

	errorTest(
		[]string{"--cache-ds", "cache", dbSpecString + "::in-ds", "out-ds"},
		"Input cannot be a commit.",
	)

	errorTest(
		[]string{"--cache-ds", "cache", dbSpecString + "::not-there.value", "out-ds"},
		"Could not find referenced value",
	)
}

func TestDownloadBlob(t *testing.T) {
	assert := assert.New(t)
	db := datas.NewDatabase(chunks.NewMemoryStore())

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, client, url: "+r.URL.String())
	}))
	defer ts.Close()

	r, err := downloadBlob(db, ts.URL+"/one")
	assert.NoError(err)

	blob := r.TargetValue(db)
	assert.NotNil(blob)
	assert.Equal("Hello, client, url: /one", stringFromBlob(blob.(types.Blob)))
}

func TestPinPath(t *testing.T) {
	assert := assert.New(t)
	db := datas.NewDatabase(chunks.NewMemoryStore())

	dsName := "testds"
	ds := db.GetDataset(dsName)
	ds, err := db.CommitValue(ds, types.NewMap(
		types.String("k1"), types.String("v1"),
		types.String("k2"), types.String("v2"),
	))
	assert.NoError(err)

	absPath, err := spec.NewAbsolutePath(`testds.value["k1"]`)
	assert.NoError(err)

	// call pin path on AbsolutePath containing dataset
	pinnedPath := pinPath(db, absPath)
	assert.Equal(`#e4ahkeask7na1s3okcp3rmqt89rkv928.value["k1"]`, pinnedPath.String())

	// call pin path on AbsolutePath that is already pinned
	pinnedPath = pinPath(db, pinnedPath)
	assert.Equal(`#e4ahkeask7na1s3okcp3rmqt89rkv928.value["k1"]`, pinnedPath.String())

	assert.True(types.String("v1").Equals(pinnedPath.Resolve(db)))
}

func TestGetLastInRoot(t *testing.T) {
	assert := assert.New(t)
	db := datas.NewDatabase(chunks.NewMemoryStore())

	k1 := types.String("k1")

	// commit source ds with no sourcePath field
	sourceM := types.NewMap(k1, types.String("source commit 1"))
	sourceDs := db.GetDataset("test-source-ds")
	sourceDs, err := db.Commit(sourceDs, sourceM, datas.CommitOptions{})
	assert.NoError(err)
	lastInRoot := getLastInRoot(db, sourceDs.Head())
	assert.Nil(lastInRoot)

	pinnedPath, err := spec.NewAbsolutePath("test-source-ds.value")
	assert.NoError(err)

	// commit dest ds with valid sourcePath field on meta
	destDs := db.GetDataset("test-dest-ds")
	destM := sourceM.Set(k1, types.String("dest commit 1"))
	meta := newMeta(db, pinnedPath.String())
	destDs, err = db.Commit(destDs, destM, datas.CommitOptions{Meta: meta})
	assert.NoError(err)
	root := getLastInRoot(db, destDs.Head())
	assert.True(sourceM.Equals(root))

	// commit with unparsable path
	destM = destM.Set(k1, types.String("dest commit 2"))
	meta = newMeta(db, "bad path")
	destDs, err = db.Commit(destDs, destM, datas.CommitOptions{Meta: meta})
	assert.NoError(err)
	root = getLastInRoot(db, destDs.Head())
	assert.Nil(lastInRoot)

	// commit with unresolveable path
	meta = newMeta(db, "#12345678901234567890123456789012.value")
	destDs, err = db.Commit(destDs, destM, datas.CommitOptions{Meta: meta})
	assert.NoError(err)
	root = getLastInRoot(db, destDs.Head())
}

func lastLine(output string) string {
	lines := strings.Split(output, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if len(lines[i]) > 0 {
			return lines[i]
		}
	}
	return ""
}
