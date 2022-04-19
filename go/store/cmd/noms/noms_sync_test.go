// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/spec"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/clienttest"
)

func TestSync(t *testing.T) {
	suite.Run(t, &nomsSyncTestSuite{})
}

type nomsSyncTestSuite struct {
	clienttest.ClientTestSuite
}

func (s *nomsSyncTestSuite) TestSyncValidation() {
	cs, err := nbs.NewLocalStore(context.Background(), types.Format_Default.VersionString(), s.DBDir, clienttest.DefaultMemTableSize, nbs.NewUnlimitedMemQuotaProvider())
	s.NoError(err)
	sourceDB := datas.NewDatabase(cs)
	source1, err := sourceDB.GetDataset(context.Background(), "src")
	s.NoError(err)
	source1, err = datas.CommitValue(context.Background(), sourceDB, source1, types.Float(42))
	s.NoError(err)
	ref, ok, err := source1.MaybeHeadRef()
	s.NoError(err)
	s.True(ok)
	source1HeadRef := ref.TargetHash()
	source1.Database().Close()
	sourceSpecMissingHashSymbol := spec.CreateValueSpecString("nbs", s.DBDir, source1HeadRef.String())

	sinkDatasetSpec := spec.CreateValueSpecString("nbs", s.DBDir2, "dest")

	defer func() {
		err := recover()
		s.Equal(clienttest.ExitError{Code: 1}, err)
	}()

	s.MustRun(main, []string{"sync", sourceSpecMissingHashSymbol, sinkDatasetSpec})
}

func (s *nomsSyncTestSuite) TestSync() {
	defer s.NoError(file.RemoveAll(s.DBDir2))

	cs, err := nbs.NewLocalStore(context.Background(), types.Format_Default.VersionString(), s.DBDir, clienttest.DefaultMemTableSize, nbs.NewUnlimitedMemQuotaProvider())
	s.NoError(err)
	sourceDB := datas.NewDatabase(cs)
	source1, err := sourceDB.GetDataset(context.Background(), "src")
	s.NoError(err)
	source1, err = datas.CommitValue(context.Background(), sourceDB, source1, types.Float(42))
	s.NoError(err)
	ref, ok, err := source1.MaybeHeadRef()
	s.NoError(err)
	s.True(ok)
	source1HeadRef := ref.TargetHash()
	s.NoError(err)
	source1, err = datas.CommitValue(context.Background(), sourceDB, source1, types.Float(43))
	s.NoError(err)
	sourceDB.Close()

	// Pull from a hash to a not-yet-existing dataset in a new DB
	sourceSpec := spec.CreateValueSpecString("nbs", s.DBDir, "#"+source1HeadRef.String())
	sinkDatasetSpec := spec.CreateValueSpecString("nbs", s.DBDir2, "dest")
	sout, _ := s.MustRun(main, []string{"sync", sourceSpec, sinkDatasetSpec})
	s.Regexp("Synced", sout)

	cs, err = nbs.NewLocalStore(context.Background(), types.Format_Default.VersionString(), s.DBDir2, clienttest.DefaultMemTableSize, nbs.NewUnlimitedMemQuotaProvider())
	s.NoError(err)
	db := datas.NewDatabase(cs)
	dest, err := db.GetDataset(context.Background(), "dest")
	s.NoError(err)
	s.True(types.Float(42).Equals(mustHeadValue(dest)))
	db.Close()

	// Pull from a dataset in one DB to an existing dataset in another
	sourceDataset := spec.CreateValueSpecString("nbs", s.DBDir, "src")
	sout, _ = s.MustRun(main, []string{"sync", sourceDataset, sinkDatasetSpec})
	s.Regexp("Synced", sout)

	cs, err = nbs.NewLocalStore(context.Background(), types.Format_Default.VersionString(), s.DBDir2, clienttest.DefaultMemTableSize, nbs.NewUnlimitedMemQuotaProvider())
	s.NoError(err)
	db = datas.NewDatabase(cs)
	dest, err = db.GetDataset(context.Background(), "dest")
	s.NoError(err)
	s.True(types.Float(43).Equals(mustHeadValue(dest)))
	db.Close()

	// Pull when sink dataset is already up to date
	sout, _ = s.MustRun(main, []string{"sync", sourceDataset, sinkDatasetSpec})
	s.Regexp("up to date", sout)

	// Pull from a source dataset to a not-yet-existing dataset in another DB, BUT all the needed chunks already exists in the sink.
	sinkDatasetSpec = spec.CreateValueSpecString("nbs", s.DBDir2, "dest2")
	sout, _ = s.MustRun(main, []string{"sync", sourceDataset, sinkDatasetSpec})
	s.Regexp("Created", sout)

	cs, err = nbs.NewLocalStore(context.Background(), types.Format_Default.VersionString(), s.DBDir2, clienttest.DefaultMemTableSize, nbs.NewUnlimitedMemQuotaProvider())
	s.NoError(err)
	db = datas.NewDatabase(cs)
	dest, err = db.GetDataset(context.Background(), "dest2")
	s.NoError(err)
	s.True(types.Float(43).Equals(mustHeadValue(dest)))
	db.Close()
}

func (s *nomsSyncTestSuite) TestSync_Issue2598() {
	defer s.NoError(file.RemoveAll(s.DBDir2))

	cs, err := nbs.NewLocalStore(context.Background(), types.Format_Default.VersionString(), s.DBDir, clienttest.DefaultMemTableSize, nbs.NewUnlimitedMemQuotaProvider())
	s.NoError(err)
	sourceDB := datas.NewDatabase(cs)
	// Create dataset "src1", which has a lineage of two commits.
	source1, err := sourceDB.GetDataset(context.Background(), "src1")
	s.NoError(err)
	source1, err = datas.CommitValue(context.Background(), sourceDB, source1, types.Float(42))
	s.NoError(err)
	source1, err = datas.CommitValue(context.Background(), sourceDB, source1, types.Float(43))
	s.NoError(err)

	// Create dataset "src2", with a lineage of one commit.
	source2, err := sourceDB.GetDataset(context.Background(), "src2")
	s.NoError(err)
	source2, err = datas.CommitValue(context.Background(), sourceDB, source2, types.Float(1))
	s.NoError(err)

	sourceDB.Close() // Close Database backing both Datasets

	// Sync over "src1"
	sourceDataset := spec.CreateValueSpecString("nbs", s.DBDir, "src1")
	sinkDatasetSpec := spec.CreateValueSpecString("nbs", s.DBDir2, "dest")
	sout, _ := s.MustRun(main, []string{"sync", sourceDataset, sinkDatasetSpec})
	cs, err = nbs.NewLocalStore(context.Background(), types.Format_Default.VersionString(), s.DBDir2, clienttest.DefaultMemTableSize, nbs.NewUnlimitedMemQuotaProvider())
	s.NoError(err)
	db := datas.NewDatabase(cs)
	dest, err := db.GetDataset(context.Background(), "dest")
	s.NoError(err)
	s.True(types.Float(43).Equals(mustHeadValue(dest)))
	db.Close()

	// Now, try syncing a second dataset. This crashed in issue #2598
	sourceDataset2 := spec.CreateValueSpecString("nbs", s.DBDir, "src2")
	sinkDatasetSpec2 := spec.CreateValueSpecString("nbs", s.DBDir2, "dest2")
	sout, _ = s.MustRun(main, []string{"sync", sourceDataset2, sinkDatasetSpec2})
	cs, err = nbs.NewLocalStore(context.Background(), types.Format_Default.VersionString(), s.DBDir2, clienttest.DefaultMemTableSize, nbs.NewUnlimitedMemQuotaProvider())
	s.NoError(err)
	db = datas.NewDatabase(cs)
	dest, err = db.GetDataset(context.Background(), "dest2")
	s.NoError(err)
	s.True(types.Float(1).Equals(mustHeadValue(dest)))
	db.Close()

	sout, _ = s.MustRun(main, []string{"sync", sourceDataset, sinkDatasetSpec})
	s.Regexp("up to date", sout)
}

func (s *nomsSyncTestSuite) TestRewind() {
	var err error
	cs, err := nbs.NewLocalStore(context.Background(), types.Format_Default.VersionString(), s.DBDir, clienttest.DefaultMemTableSize, nbs.NewUnlimitedMemQuotaProvider())
	s.NoError(err)
	sourceDB := datas.NewDatabase(cs)
	src, err := sourceDB.GetDataset(context.Background(), "foo")
	s.NoError(err)
	src, err = datas.CommitValue(context.Background(), sourceDB, src, types.Float(42))
	s.NoError(err)
	rewindRef := mustHeadAddr(src)
	src, err = datas.CommitValue(context.Background(), sourceDB, src, types.Float(43))
	s.NoError(err)
	sourceDB.Close() // Close Database backing both Datasets

	sourceSpec := spec.CreateValueSpecString("nbs", s.DBDir, "#"+rewindRef.String())
	sinkDatasetSpec := spec.CreateValueSpecString("nbs", s.DBDir, "foo")
	s.MustRun(main, []string{"sync", sourceSpec, sinkDatasetSpec})

	cs, err = nbs.NewLocalStore(context.Background(), types.Format_Default.VersionString(), s.DBDir, clienttest.DefaultMemTableSize, nbs.NewUnlimitedMemQuotaProvider())
	s.NoError(err)
	db := datas.NewDatabase(cs)
	dest, err := db.GetDataset(context.Background(), "foo")
	s.NoError(err)
	s.True(types.Float(42).Equals(mustHeadValue(dest)))
	db.Close()
}
