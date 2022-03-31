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

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/spec"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/clienttest"
)

func TestDs(t *testing.T) {
	suite.Run(t, &nomsDsTestSuite{})
}

type nomsDsTestSuite struct {
	clienttest.ClientTestSuite
}

func (s *nomsDsTestSuite) TestEmptyNomsDs() {
	dir := s.DBDir

	cs, err := nbs.NewLocalStore(context.Background(), types.Format_Default.VersionString(), dir, clienttest.DefaultMemTableSize, nbs.NewUnlimitedMemQuotaProvider())
	s.NoError(err)
	ds := datas.NewDatabase(cs)

	ds.Close()

	dbSpec := spec.CreateDatabaseSpecString("nbs", dir)
	rtnVal, _ := s.MustRun(main, []string{"ds", dbSpec})
	s.Equal("", rtnVal)
}

func (s *nomsDsTestSuite) TestNomsDs() {
	dir := s.DBDir

	cs, err := nbs.NewLocalStore(context.Background(), types.Format_Default.VersionString(), dir, clienttest.DefaultMemTableSize, nbs.NewUnlimitedMemQuotaProvider())
	s.NoError(err)
	db := datas.NewDatabase(cs)

	id := "testdataset"
	set, err := db.GetDataset(context.Background(), id)
	s.NoError(err)
	set, err = datas.CommitValue(context.Background(), db, set, types.String("Commit Value"))
	s.NoError(err)

	id2 := "testdataset2"
	set2, err := db.GetDataset(context.Background(), id2)
	s.NoError(err)
	set2, err = datas.CommitValue(context.Background(), db, set2, types.String("Commit Value2"))
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
	s.Equal("Deleted "+datasetName+" (was #oetp3jigkp5pid2f5c4mknpo17mso31b)\n", rtnVal)

	// print datasets, just one left
	rtnVal, _ = s.MustRun(main, []string{"ds", dbSpec})
	s.Equal(id2+"\n", rtnVal)

	// delete the second dataset
	rtnVal, _ = s.MustRun(main, []string{"ds", "-d", dataset2Name})
	s.Equal("Deleted "+dataset2Name+" (was #tsbj1qq88llk3k8qqqb5n3188sbpiu7r)\n", rtnVal)

	// print datasets, none left
	rtnVal, _ = s.MustRun(main, []string{"ds", dbSpec})
	s.Equal("", rtnVal)
}
