// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

func TestDs(t *testing.T) {
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
	rtnVal, _ := s.MustRun(main, []string{"ds", dbSpec})
	s.Equal("", rtnVal)
}

func (s *nomsDsTestSuite) TestNomsDs() {
	dir := s.LdbDir

	cs := chunks.NewLevelDBStore(dir+"/name", "", 24, false)
	db := datas.NewDatabase(cs)

	id := "testdataset"
	set := db.GetDataset(id)
	set, err := db.CommitValue(set, types.String("Commit Value"))
	s.NoError(err)

	id2 := "testdataset2"
	set2 := db.GetDataset(id2)
	set2, err = db.CommitValue(set2, types.String("Commit Value2"))
	s.NoError(err)

	err = db.Close()
	s.NoError(err)

	dbSpec := spec.CreateDatabaseSpecString("ldb", dir+"/name")
	datasetName := spec.CreateValueSpecString("ldb", dir+"/name", id)
	dataset2Name := spec.CreateValueSpecString("ldb", dir+"/name", id2)

	// both datasets show up
	rtnVal, _ := s.MustRun(main, []string{"ds", dbSpec})
	s.Equal(id+"\n"+id2+"\n", rtnVal)

	// both datasets again, to make sure printing doesn't change them
	rtnVal, _ = s.MustRun(main, []string{"ds", dbSpec})
	s.Equal(id+"\n"+id2+"\n", rtnVal)

	// delete one dataset, print message at delete
	rtnVal, _ = s.MustRun(main, []string{"ds", "-d", datasetName})
	s.Equal("Deleted "+datasetName+" (was #6ebc05f71q4sk2psi534fom9se228161)\n", rtnVal)

	// print datasets, just one left
	rtnVal, _ = s.MustRun(main, []string{"ds", dbSpec})
	s.Equal(id2+"\n", rtnVal)

	// delete the second dataset
	rtnVal, _ = s.MustRun(main, []string{"ds", "-d", dataset2Name})
	s.Equal("Deleted "+dataset2Name+" (was #f5qtovr9mv7mjj2uoq7flcfpksgf0s2j)\n", rtnVal)

	// print datasets, none left
	rtnVal, _ = s.MustRun(main, []string{"ds", dbSpec})
	s.Equal("", rtnVal)
}
