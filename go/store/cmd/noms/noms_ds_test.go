// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/constants"
	"github.com/liquidata-inc/ld/dolt/go/store/datas"
	"github.com/liquidata-inc/ld/dolt/go/store/nbs"
	"github.com/liquidata-inc/ld/dolt/go/store/spec"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/liquidata-inc/ld/dolt/go/store/util/clienttest"
	"github.com/stretchr/testify/suite"
)

func TestDs(t *testing.T) {
	suite.Run(t, &nomsDsTestSuite{})
}

type nomsDsTestSuite struct {
	clienttest.ClientTestSuite
}

func (s *nomsDsTestSuite) TestEmptyNomsDs() {
	dir := s.DBDir

	cs, err := nbs.NewLocalStore(context.Background(), constants.DefaultNomsBinFormat, dir, clienttest.DefaultMemTableSize)
	s.NoError(err)
	ds := datas.NewDatabase(cs)

	ds.Close()

	dbSpec := spec.CreateDatabaseSpecString("nbs", dir)
	rtnVal, _ := s.MustRun(main, []string{"ds", dbSpec})
	s.Equal("", rtnVal)
}

func (s *nomsDsTestSuite) TestNomsDs() {
	dir := s.DBDir

	cs, err := nbs.NewLocalStore(context.Background(), constants.DefaultNomsBinFormat, dir, clienttest.DefaultMemTableSize)
	s.NoError(err)
	db := datas.NewDatabase(cs)

	id := "testdataset"
	set, err := db.GetDataset(context.Background(), id)
	s.NoError(err)
	set, err = db.CommitValue(context.Background(), set, types.String("Commit Value"))
	s.NoError(err)

	id2 := "testdataset2"
	set2, err := db.GetDataset(context.Background(), id2)
	s.NoError(err)
	set2, err = db.CommitValue(context.Background(), set2, types.String("Commit Value2"))
	s.NoError(err)

	err = db.Close()
	s.NoError(err)

	dbSpec := spec.CreateDatabaseSpecString("nbs", dir)
	datasetName := spec.CreateValueSpecString("nbs", dir, id)
	dataset2Name := spec.CreateValueSpecString("nbs", dir, id2)

	// both datasets show up
	rtnVal, _ := s.MustRun(main, []string{"ds", dbSpec})
	s.Equal(id+"\n"+id2+"\n", rtnVal)

	// both datasets again, to make sure printing doesn't change them
	rtnVal, _ = s.MustRun(main, []string{"ds", dbSpec})
	s.Equal(id+"\n"+id2+"\n", rtnVal)

	// delete one dataset, print message at delete
	rtnVal, _ = s.MustRun(main, []string{"ds", "-d", datasetName})
	s.Equal("Deleted "+datasetName+" (was #ld4fuj44sd4gu0pepn7h5hga72282v81)\n", rtnVal)

	// print datasets, just one left
	rtnVal, _ = s.MustRun(main, []string{"ds", dbSpec})
	s.Equal(id2+"\n", rtnVal)

	// delete the second dataset
	rtnVal, _ = s.MustRun(main, []string{"ds", "-d", dataset2Name})
	s.Equal("Deleted "+dataset2Name+" (was #43qqlvkiainn1jf53g705622nndu1bje)\n", rtnVal)

	// print datasets, none left
	rtnVal, _ = s.MustRun(main, []string{"ds", dbSpec})
	s.Equal("", rtnVal)
}
