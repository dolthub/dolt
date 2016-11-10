// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

func randomBytes(blen int) []byte {
	key := make([]byte, blen)

	_, err := rand.Read(key)
	d.Chk.NoError(err)
	return key
}

func randomString(slen int) string {
	bs := randomBytes(slen)
	return base64.StdEncoding.EncodeToString(bs)
}

func randomBlob(s *resourceCacheTestSuite, slen int) types.Blob {
	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("ldb", s.LdbDir))
	s.NoError(err)
	defer sp.Close()

	s1 := randomString(slen)
	blob := types.NewStreamingBlob(sp.GetDatabase(), strings.NewReader(s1))
	return blob
}

func stringFromBlob(blob types.Blob) string {
	buf := new(bytes.Buffer)
	buf.ReadFrom(blob.Reader())
	return buf.String()
}

type resourceCacheTestSuite struct {
	clienttest.ClientTestSuite
}

func TestResourceCache(t *testing.T) {
	suite.Run(t, &resourceCacheTestSuite{})
}

func (s *resourceCacheTestSuite) TestResourceCacheGet() {
	dsName := "testCache"
	cache1 := func(k types.String, v types.Blob, setNewValue bool) (types.Ref, types.Ref) {
		sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("ldb", s.LdbDir))
		s.NoError(err)
		defer sp.Close()

		db := sp.GetDatabase()

		hr, _ := db.GetDataset(dsName).MaybeHeadRef()
		rc, err := getResourceCache(db, dsName)
		s.NoError(err)

		r := db.WriteValue(v)
		if setNewValue {
			rc.set(k, r)
		}
		cachedVal, ok := rc.get(k)
		s.True(ok)
		s.Equal(r, cachedVal)

		err = rc.commit(db, dsName)
		s.NoError(err)
		hr1 := db.GetDataset(dsName).HeadRef()
		return hr, hr1
	}

	blob1 := randomBlob(s, 30)
	blob2 := randomBlob(s, 30)
	s1 := stringFromBlob(blob1)

	hr1, hr2 := cache1(types.String("key1"), blob1, true)
	s.True(types.Ref{} == hr1)
	s.False(types.Ref{} == hr2)

	hr1, hr2 = cache1(types.String("key1"), blob1, false)
	s.True(hr1.Equals(hr2))

	hr1, hr2 = cache1(types.String("key2"), blob2, true)
	s.False(hr1.Equals(hr2))

	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("ldb", s.LdbDir))
	s.NoError(err)
	defer sp.Close()

	db := sp.GetDatabase()

	rc, err := getResourceCache(db, dsName)
	s.NoError(err)
	s.Equal(uint64(2), rc.len())
	br1, _ := rc.get("key1")
	b1 := br1.TargetValue(db).(types.Blob)
	s2 := stringFromBlob(b1)

	s.Equal(s1, s2)
}

func (s *resourceCacheTestSuite) TestCheckCacheType() {
	blob1 := randomBlob(s, 30)

	badTestCases := []types.Value{
		types.NewStruct("testStruct", types.StructData{"f1": types.String("f1value")}),
		types.NewMap(types.Number(1), types.NewRef(blob1)),
		types.NewMap(types.String("s1"), types.String("badtype")),
		types.NewMap(types.String("s1"), types.NewRef(types.String("badtype"))),
	}

	for _, tc := range badTestCases {
		err := checkCacheType(tc)
		s.Error(err)
	}

	c1 := types.NewMap(types.String("s1"), types.NewRef(blob1))
	err := checkCacheType(c1)
	s.NoError(err)
}
