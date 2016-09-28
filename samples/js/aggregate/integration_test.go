// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package aggregate

import (
	"testing"

	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/util/integrationtest"
)

const inDsName = "test-agg-in"
const outDsName = "test-agg-out"

func TestIntegration(t *testing.T) {
	integrationtest.Run(t, &testSuite{})
}

type testSuite struct {
	integrationtest.IntegrationSuite
}

type TestStruct struct {
	Year string
}

func (s *testSuite) Setup() {
	db := s.Database()
	defer db.Close()
	ds := db.GetDataset(inDsName)

	testStructs := []TestStruct{
		{"2012"},
		{"2012"},
		{"2013"},
		{"2012"},
		{"2013"},
		{"2014"},
		{"2012"},
		{"2013"},
		{"2014"},
		{"2015"},
	}

	inData, err := marshal.Marshal(testStructs)
	s.NoError(err)
	_, err = db.CommitValue(ds, inData)
	s.NoError(err)
}

func (s *testSuite) Teardown() {
	db := s.Database()
	defer db.Close()
	outData := db.GetDataset(outDsName).HeadValue()
	expectedMap := map[string]int{
		"2012": 4,
		"2013": 3,
		"2014": 2,
		"2015": 1,
	}
	expected, err := marshal.Marshal(expectedMap)
	s.NoError(err)
	s.True(expected.Equals(outData))
}

func (s *testSuite) NodeArgs() []string {
	inDsSpec := s.ValueSpecString(inDsName)
	outDsSpec := s.ValueSpecString(outDsName)
	return []string{"-s", "TestStruct", "-g", "year", inDsSpec, outDsSpec}
}
