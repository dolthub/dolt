// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

func TestDs(t *testing.T) {
	d.UtilExiter = testExiter{}
	suite.Run(t, &nomsDsTestSuite{})
}

type nomsDsTestSuite struct {
	clienttest.ClientTestSuite
}

func (s *nomsDsTestSuite) TestEmptyNomsDs() {
	dir := s.LdbDir

	cs := chunks.NewLevelDBStore(dir+"/name", "", 24, false)
	ds := datas.NewDatabase(cs)

	ds.Close()

	dbSpec := spec.CreateDatabaseSpecString("ldb", dir+"/name")
	rtnVal, _ := s.Run(main, []string{"ds", dbSpec})
	s.Equal("", rtnVal)
}

func (s *nomsDsTestSuite) TestNomsDs() {
	dir := s.LdbDir

	cs := chunks.NewLevelDBStore(dir+"/name", "", 24, false)
	ds := datas.NewDatabase(cs)

	id := "testdataset"
	set := dataset.NewDataset(ds, id)
	set, err := set.Commit(types.String("Commit Value"))
	s.NoError(err)

	id2 := "testdataset2"
	set2 := dataset.NewDataset(ds, id2)
	set2, err = set2.Commit(types.String("Commit Value2"))
	s.NoError(err)

	err = ds.Close()
	s.NoError(err)

	dbSpec := spec.CreateDatabaseSpecString("ldb", dir+"/name")
	datasetName := spec.CreateValueSpecString("ldb", dir+"/name", id)
	dataset2Name := spec.CreateValueSpecString("ldb", dir+"/name", id2)

	// both datasets show up
	rtnVal, _ := s.Run(main, []string{"ds", dbSpec})
	s.Equal(id+"\n"+id2+"\n", rtnVal)

	// both datasets again, to make sure printing doesn't change them
	rtnVal, _ = s.Run(main, []string{"ds", dbSpec})
	s.Equal(id+"\n"+id2+"\n", rtnVal)

	// delete one dataset, print message at delete
	rtnVal, _ = s.Run(main, []string{"ds", "-d", datasetName})
	s.Equal("Deleted dataset "+id+" (was sha1-d54b79552cda9ebe8e446eeb19aab0e69b6ceee3)\n\n", rtnVal)

	// print datasets, just one left
	rtnVal, _ = s.Run(main, []string{"ds", dbSpec})
	s.Equal(id2+"\n", rtnVal)

	// delete the second dataset
	rtnVal, _ = s.Run(main, []string{"ds", "-d", dataset2Name})
	s.Equal("Deleted dataset "+id2+" (was sha1-7b75b0ebfc2a0815ba6fb2b31d03c8f9976ae530)\n\n", rtnVal)

	// print datasets, none left
	rtnVal, _ = s.Run(main, []string{"ds", dbSpec})
	s.Equal("", rtnVal)
}
