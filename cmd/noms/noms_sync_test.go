// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"os"
	"testing"

	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/nbs"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

func TestSync(t *testing.T) {
	suite.Run(t, &nomsSyncTestSuite{})
}

type nomsSyncTestSuite struct {
	clienttest.ClientTestSuite
}

func (s *nomsSyncTestSuite) TestSyncValidation() {
	sourceDB := datas.NewDatabase(nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize))
	source1 := sourceDB.GetDataset("src")
	source1, err := sourceDB.CommitValue(source1, types.Number(42))
	s.NoError(err)
	source1HeadRef := source1.Head().Hash()
	source1.Database().Close()
	sourceSpecMissingHashSymbol := spec.CreateValueSpecString("nbs", s.DBDir, source1HeadRef.String())

	sinkDatasetSpec := spec.CreateValueSpecString("nbs", s.DBDir2, "dest")

	defer func() {
		err := recover()
		s.Equal(clienttest.ExitError{1}, err)
	}()

	s.MustRun(main, []string{"sync", sourceSpecMissingHashSymbol, sinkDatasetSpec})
}

func (s *nomsSyncTestSuite) TestSync() {
	defer s.NoError(os.RemoveAll(s.DBDir2))

	sourceDB := datas.NewDatabase(nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize))
	source1 := sourceDB.GetDataset("src")
	source1, err := sourceDB.CommitValue(source1, types.Number(42))
	s.NoError(err)
	source1HeadRef := source1.Head().Hash() // Remember first head, so we can sync to it.
	source1, err = sourceDB.CommitValue(source1, types.Number(43))
	s.NoError(err)
	sourceDB.Close()

	// Pull from a hash to a not-yet-existing dataset in a new DB
	sourceSpec := spec.CreateValueSpecString("nbs", s.DBDir, "#"+source1HeadRef.String())
	sinkDatasetSpec := spec.CreateValueSpecString("nbs", s.DBDir2, "dest")
	sout, _ := s.MustRun(main, []string{"sync", sourceSpec, sinkDatasetSpec})
	s.Regexp("Synced", sout)

	db := datas.NewDatabase(nbs.NewLocalStore(s.DBDir2, clienttest.DefaultMemTableSize))
	dest := db.GetDataset("dest")
	s.True(types.Number(42).Equals(dest.HeadValue()))
	db.Close()

	// Pull from a dataset in one DB to an existing dataset in another
	sourceDataset := spec.CreateValueSpecString("nbs", s.DBDir, "src")
	sout, _ = s.MustRun(main, []string{"sync", sourceDataset, sinkDatasetSpec})
	s.Regexp("Synced", sout)

	db = datas.NewDatabase(nbs.NewLocalStore(s.DBDir2, clienttest.DefaultMemTableSize))
	dest = db.GetDataset("dest")
	s.True(types.Number(43).Equals(dest.HeadValue()))
	db.Close()

	// Pull when sink dataset is already up to date
	sout, _ = s.MustRun(main, []string{"sync", sourceDataset, sinkDatasetSpec})
	s.Regexp("up to date", sout)

	// Pull from a source dataset to a not-yet-existing dataset in another DB, BUT all the needed chunks already exists in the sink.
	sinkDatasetSpec = spec.CreateValueSpecString("nbs", s.DBDir2, "dest2")
	sout, _ = s.MustRun(main, []string{"sync", sourceDataset, sinkDatasetSpec})
	s.Regexp("Created", sout)

	db = datas.NewDatabase(nbs.NewLocalStore(s.DBDir2, clienttest.DefaultMemTableSize))
	dest = db.GetDataset("dest2")
	s.True(types.Number(43).Equals(dest.HeadValue()))
	db.Close()
}

func (s *nomsSyncTestSuite) TestSync_Issue2598() {
	defer s.NoError(os.RemoveAll(s.DBDir2))

	sourceDB := datas.NewDatabase(nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize))
	// Create dataset "src1", which has a lineage of two commits.
	source1 := sourceDB.GetDataset("src1")
	source1, err := sourceDB.CommitValue(source1, types.Number(42))
	s.NoError(err)
	source1, err = sourceDB.CommitValue(source1, types.Number(43))
	s.NoError(err)

	// Create dataset "src2", with a lineage of one commit.
	source2 := sourceDB.GetDataset("src2")
	source2, err = sourceDB.CommitValue(source2, types.Number(1))
	s.NoError(err)

	sourceDB.Close() // Close Database backing both Datasets

	// Sync over "src1"
	sourceDataset := spec.CreateValueSpecString("nbs", s.DBDir, "src1")
	sinkDatasetSpec := spec.CreateValueSpecString("nbs", s.DBDir2, "dest")
	sout, _ := s.MustRun(main, []string{"sync", sourceDataset, sinkDatasetSpec})

	db := datas.NewDatabase(nbs.NewLocalStore(s.DBDir2, clienttest.DefaultMemTableSize))
	dest := db.GetDataset("dest")
	s.True(types.Number(43).Equals(dest.HeadValue()))
	db.Close()

	// Now, try syncing a second dataset. This crashed in issue #2598
	sourceDataset2 := spec.CreateValueSpecString("nbs", s.DBDir, "src2")
	sinkDatasetSpec2 := spec.CreateValueSpecString("nbs", s.DBDir2, "dest2")
	sout, _ = s.MustRun(main, []string{"sync", sourceDataset2, sinkDatasetSpec2})

	db = datas.NewDatabase(nbs.NewLocalStore(s.DBDir2, clienttest.DefaultMemTableSize))
	dest = db.GetDataset("dest2")
	s.True(types.Number(1).Equals(dest.HeadValue()))
	db.Close()

	sout, _ = s.MustRun(main, []string{"sync", sourceDataset, sinkDatasetSpec})
	s.Regexp("up to date", sout)
}

func (s *nomsSyncTestSuite) TestRewind() {
	var err error
	sourceDB := datas.NewDatabase(nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize))
	src := sourceDB.GetDataset("foo")
	src, err = sourceDB.CommitValue(src, types.Number(42))
	s.NoError(err)
	rewindRef := src.HeadRef().TargetHash()
	src, err = sourceDB.CommitValue(src, types.Number(43))
	s.NoError(err)
	sourceDB.Close() // Close Database backing both Datasets

	sourceSpec := spec.CreateValueSpecString("nbs", s.DBDir, "#"+rewindRef.String())
	sinkDatasetSpec := spec.CreateValueSpecString("nbs", s.DBDir, "foo")
	s.MustRun(main, []string{"sync", sourceSpec, sinkDatasetSpec})

	db := datas.NewDatabase(nbs.NewLocalStore(s.DBDir, clienttest.DefaultMemTableSize))
	dest := db.GetDataset("foo")
	s.True(types.Number(42).Equals(dest.HeadValue()))
	db.Close()
}
