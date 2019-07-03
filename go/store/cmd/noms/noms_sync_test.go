// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"os"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/datas"
	"github.com/liquidata-inc/ld/dolt/go/store/nbs"
	"github.com/liquidata-inc/ld/dolt/go/store/spec"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/liquidata-inc/ld/dolt/go/store/util/clienttest"
	"github.com/stretchr/testify/suite"
)

func TestSync(t *testing.T) {
	suite.Run(t, &nomsSyncTestSuite{})
}

type nomsSyncTestSuite struct {
	clienttest.ClientTestSuite
}

func (s *nomsSyncTestSuite) TestSyncValidation() {
	cs, err := nbs.NewLocalStore(context.Background(), s.DBDir, clienttest.DefaultMemTableSize)
	s.NoError(err)
	sourceDB := datas.NewDatabase(cs)
	source1 := sourceDB.GetDataset(context.Background(), "src")
	source1, err = sourceDB.CommitValue(context.Background(), source1, types.Float(42))
	s.NoError(err)
	// TODO(binformat)
	source1HeadRef := source1.Head().Hash(types.Format_7_18)
	source1.Database().Close()
	sourceSpecMissingHashSymbol := spec.CreateValueSpecString(types.Format_7_18, "nbs", s.DBDir, source1HeadRef.String())

	sinkDatasetSpec := spec.CreateValueSpecString(types.Format_7_18, "nbs", s.DBDir2, "dest")

	defer func() {
		err := recover()
		s.Equal(clienttest.ExitError{1}, err)
	}()

	s.MustRun(main, []string{"sync", sourceSpecMissingHashSymbol, sinkDatasetSpec})
}

func (s *nomsSyncTestSuite) TestSync() {
	defer s.NoError(os.RemoveAll(s.DBDir2))

	cs, err := nbs.NewLocalStore(context.Background(), s.DBDir, clienttest.DefaultMemTableSize)
	s.NoError(err)
	sourceDB := datas.NewDatabase(cs)
	source1 := sourceDB.GetDataset(context.Background(), "src")
	source1, err = sourceDB.CommitValue(context.Background(), source1, types.Float(42))
	s.NoError(err)
	// TODO(binformat)
	source1HeadRef := source1.Head().Hash(types.Format_7_18) // Remember first head, so we can sync to it.
	source1, err = sourceDB.CommitValue(context.Background(), source1, types.Float(43))
	s.NoError(err)
	sourceDB.Close()

	// Pull from a hash to a not-yet-existing dataset in a new DB
	sourceSpec := spec.CreateValueSpecString(types.Format_7_18, "nbs", s.DBDir, "#"+source1HeadRef.String())
	sinkDatasetSpec := spec.CreateValueSpecString(types.Format_7_18, "nbs", s.DBDir2, "dest")
	sout, _ := s.MustRun(main, []string{"sync", sourceSpec, sinkDatasetSpec})
	s.Regexp("Synced", sout)

	cs, err = nbs.NewLocalStore(context.Background(), s.DBDir2, clienttest.DefaultMemTableSize)
	s.NoError(err)
	db := datas.NewDatabase(cs)
	dest := db.GetDataset(context.Background(), "dest")
	s.True(types.Float(42).Equals(dest.HeadValue()))
	db.Close()

	// Pull from a dataset in one DB to an existing dataset in another
	sourceDataset := spec.CreateValueSpecString(types.Format_7_18, "nbs", s.DBDir, "src")
	sout, _ = s.MustRun(main, []string{"sync", sourceDataset, sinkDatasetSpec})
	s.Regexp("Synced", sout)

	cs, err = nbs.NewLocalStore(context.Background(), s.DBDir2, clienttest.DefaultMemTableSize)
	s.NoError(err)
	db = datas.NewDatabase(cs)
	dest = db.GetDataset(context.Background(), "dest")
	s.True(types.Float(43).Equals(dest.HeadValue()))
	db.Close()

	// Pull when sink dataset is already up to date
	sout, _ = s.MustRun(main, []string{"sync", sourceDataset, sinkDatasetSpec})
	s.Regexp("up to date", sout)

	// Pull from a source dataset to a not-yet-existing dataset in another DB, BUT all the needed chunks already exists in the sink.
	sinkDatasetSpec = spec.CreateValueSpecString(types.Format_7_18, "nbs", s.DBDir2, "dest2")
	sout, _ = s.MustRun(main, []string{"sync", sourceDataset, sinkDatasetSpec})
	s.Regexp("Created", sout)

	cs, err = nbs.NewLocalStore(context.Background(), s.DBDir2, clienttest.DefaultMemTableSize)
	s.NoError(err)
	db = datas.NewDatabase(cs)
	dest = db.GetDataset(context.Background(), "dest2")
	s.True(types.Float(43).Equals(dest.HeadValue()))
	db.Close()
}

func (s *nomsSyncTestSuite) TestSync_Issue2598() {
	defer s.NoError(os.RemoveAll(s.DBDir2))

	cs, err := nbs.NewLocalStore(context.Background(), s.DBDir, clienttest.DefaultMemTableSize)
	s.NoError(err)
	sourceDB := datas.NewDatabase(cs)
	// Create dataset "src1", which has a lineage of two commits.
	source1 := sourceDB.GetDataset(context.Background(), "src1")
	source1, err = sourceDB.CommitValue(context.Background(), source1, types.Float(42))
	s.NoError(err)
	source1, err = sourceDB.CommitValue(context.Background(), source1, types.Float(43))
	s.NoError(err)

	// Create dataset "src2", with a lineage of one commit.
	source2 := sourceDB.GetDataset(context.Background(), "src2")
	source2, err = sourceDB.CommitValue(context.Background(), source2, types.Float(1))
	s.NoError(err)

	sourceDB.Close() // Close Database backing both Datasets

	// Sync over "src1"
	sourceDataset := spec.CreateValueSpecString(types.Format_7_18, "nbs", s.DBDir, "src1")
	sinkDatasetSpec := spec.CreateValueSpecString(types.Format_7_18, "nbs", s.DBDir2, "dest")
	sout, _ := s.MustRun(main, []string{"sync", sourceDataset, sinkDatasetSpec})
	cs, err = nbs.NewLocalStore(context.Background(), s.DBDir2, clienttest.DefaultMemTableSize)
	db := datas.NewDatabase(cs)
	dest := db.GetDataset(context.Background(), "dest")
	s.True(types.Float(43).Equals(dest.HeadValue()))
	db.Close()

	// Now, try syncing a second dataset. This crashed in issue #2598
	sourceDataset2 := spec.CreateValueSpecString(types.Format_7_18, "nbs", s.DBDir, "src2")
	sinkDatasetSpec2 := spec.CreateValueSpecString(types.Format_7_18, "nbs", s.DBDir2, "dest2")
	sout, _ = s.MustRun(main, []string{"sync", sourceDataset2, sinkDatasetSpec2})
	cs, err = nbs.NewLocalStore(context.Background(), s.DBDir2, clienttest.DefaultMemTableSize)
	s.NoError(err)
	db = datas.NewDatabase(cs)
	dest = db.GetDataset(context.Background(), "dest2")
	s.True(types.Float(1).Equals(dest.HeadValue()))
	db.Close()

	sout, _ = s.MustRun(main, []string{"sync", sourceDataset, sinkDatasetSpec})
	s.Regexp("up to date", sout)
}

func (s *nomsSyncTestSuite) TestRewind() {
	var err error
	cs, err := nbs.NewLocalStore(context.Background(), s.DBDir, clienttest.DefaultMemTableSize)
	s.NoError(err)
	sourceDB := datas.NewDatabase(cs)
	src := sourceDB.GetDataset(context.Background(), "foo")
	src, err = sourceDB.CommitValue(context.Background(), src, types.Float(42))
	s.NoError(err)
	rewindRef := src.HeadRef().TargetHash()
	src, err = sourceDB.CommitValue(context.Background(), src, types.Float(43))
	s.NoError(err)
	sourceDB.Close() // Close Database backing both Datasets

	sourceSpec := spec.CreateValueSpecString(types.Format_7_18, "nbs", s.DBDir, "#"+rewindRef.String())
	sinkDatasetSpec := spec.CreateValueSpecString(types.Format_7_18, "nbs", s.DBDir, "foo")
	s.MustRun(main, []string{"sync", sourceSpec, sinkDatasetSpec})

	cs, err = nbs.NewLocalStore(context.Background(), s.DBDir, clienttest.DefaultMemTableSize)
	s.NoError(err)
	db := datas.NewDatabase(cs)
	dest := db.GetDataset(context.Background(), "foo")
	s.True(types.Float(42).Equals(dest.HeadValue()))
	db.Close()
}
