// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/clients/go/test_util"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/testify/suite"
)

func TestDs(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	test_util.ClientTestSuite
}

func (s *testSuite) TestEmptyNomsDs() {
	dir := s.LdbDir

	cs := chunks.NewLevelDBStore(dir+"/name", "", 24, false)
	ds := datas.NewDatabase(cs)

	ds.Close()

	dataStoreName := "ldb:" + dir + "/name"
	rtnVal := s.Run(main, []string{dataStoreName})
	s.Equal("", rtnVal)
}

func (s *testSuite) TestNomsDs() {
	dir := s.LdbDir

	cs := chunks.NewLevelDBStore(dir+"/name", "", 24, false)
	ds := datas.NewDatabase(cs)
	id := "testdataset"

	set := dataset.NewDataset(ds, id)
	set, err := set.Commit(types.NewString("Commit Value"))
	s.NoError(err)

	id2 := "testdataset2"

	set2 := dataset.NewDataset(ds, id2)
	set2, err = set2.Commit(types.NewString("Commit Value2"))
	s.NoError(err)

	err = ds.Close()
	s.NoError(err)

	dataStoreName := "ldb:" + dir + "/name"
	datasetName := dataStoreName + ":" + id
	dataset2Name := dataStoreName + ":" + id2

	// both datasets show up
	rtnVal := s.Run(main, []string{dataStoreName})
	s.Equal(id+"\n"+id2+"\n", rtnVal)

	// both datasets again, to make sure printing doesn't change them
	rtnVal = s.Run(main, []string{dataStoreName})
	s.Equal(id+"\n"+id2+"\n", rtnVal)

	// delete one dataset, print message at delete
	rtnVal = s.Run(main, []string{"-d", datasetName})
	s.Equal("Deleted dataset "+id+" (was sha1-d54b79552cda9ebe8e446eeb19aab0e69b6ceee3)\n\n", rtnVal)

	// resetting flag because main is called multiple times
	*toDelete = ""
	// print datasets, just one left
	rtnVal = s.Run(main, []string{dataStoreName})
	s.Equal(id2+"\n", rtnVal)

	// delete the second dataset
	rtnVal = s.Run(main, []string{"-d", dataset2Name})
	s.Equal("Deleted dataset "+id2+" (was sha1-7b75b0ebfc2a0815ba6fb2b31d03c8f9976ae530)\n\n", rtnVal)

	//resetting flag because main is called multiple times
	*toDelete = ""
	// print datasets, none left
	rtnVal = s.Run(main, []string{dataStoreName})
	s.Equal("", rtnVal)
}
