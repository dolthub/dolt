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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/spec"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/clienttest"
	"github.com/dolthub/dolt/go/store/util/test"
)

func TestNomsLog(t *testing.T) {
	suite.Run(t, &nomsLogTestSuite{})
}

type nomsLogTestSuite struct {
	clienttest.ClientTestSuite
}

func testCommitInResults(s *nomsLogTestSuite, str string, i int) {
	sp, err := spec.ForDataset(str)
	s.NoError(err)
	defer sp.Close()

	db := sp.GetDatabase(context.Background())
	vrw := sp.GetVRW(context.Background())
	datas.CommitValue(context.Background(), db, sp.GetDataset(context.Background()), types.Float(i))
	s.NoError(err)

	commit, ok := sp.GetDataset(context.Background()).MaybeHead()
	s.True(ok)
	res, _ := s.MustRun(main, []string{"log", str})
	h, err := commit.Hash(vrw.Format())
	s.NoError(err)
	s.Contains(res, h.String())
}

func (s *nomsLogTestSuite) TestNomsLog() {
	sp, err := spec.ForDataset(spec.CreateValueSpecString("nbs", s.DBDir, "dsTest"))
	s.NoError(err)
	defer sp.Close()

	sp.GetDatabase(context.Background()) // create the database
	s.Panics(func() { s.MustRun(main, []string{"log", sp.String()}) })

	testCommitInResults(s, sp.String(), 1)
	testCommitInResults(s, sp.String(), 2)
}

func (s *nomsLogTestSuite) TestNomsLogPath() {
	sp, err := spec.ForPath(spec.CreateValueSpecString("nbs", s.DBDir, "dsTest.value.bar"))
	s.NoError(err)
	defer sp.Close()

	db := sp.GetDatabase(context.Background())
	vrw := sp.GetVRW(context.Background())
	ds := sp.GetDataset(context.Background())
	for i := 0; i < 3; i++ {
		data, err := types.NewStruct(vrw.Format(), "", types.StructData{
			"bar": types.Float(i),
		})
		s.NoError(err)
		ds, err = datas.CommitValue(context.Background(), db, ds, data)
		s.NoError(err)
	}

	stdout, stderr := s.MustRun(main, []string{"log", "--show-value", sp.String()})
	s.Empty(stderr)
	test.EqualsIgnoreHashes(s.T(), pathValue, stdout)

	stdout, stderr = s.MustRun(main, []string{"log", sp.String()})
	s.Empty(stderr)
	test.EqualsIgnoreHashes(s.T(), pathDiff, stdout)
}

func addCommit(ds datas.Dataset, v string) (datas.Dataset, error) {
	db := ds.Database()
	return datas.CommitValue(context.Background(), db, ds, types.String(v))
}

func addCommitWithValue(ds datas.Dataset, v types.Value) (datas.Dataset, error) {
	db := ds.Database()
	return datas.CommitValue(context.Background(), db, ds, v)
}

func addBranchedDataset(vrw types.ValueReadWriter, newDs, parentDs datas.Dataset, v string) (datas.Dataset, error) {
	return newDs.Database().Commit(context.Background(), newDs, types.String(v), datas.CommitOptions{Parents: []hash.Hash{mustHeadAddr(parentDs)}})
}

func mergeDatasets(vrw types.ValueReadWriter, ds1, ds2 datas.Dataset, v string) (datas.Dataset, error) {
	return ds1.Database().Commit(context.Background(), ds1, types.String(v), datas.CommitOptions{Parents: []hash.Hash{mustHeadAddr(ds1), mustHeadAddr(ds2)}})
}

func mustHead(ds datas.Dataset) types.Value {
	s, ok := ds.MaybeHead()
	if !ok {
		panic("no head")
	}
	return s
}

func mustHeadAddr(ds datas.Dataset) hash.Hash {
	addr, ok := ds.MaybeHeadAddr()
	d.PanicIfFalse(ok)
	return addr
}

func mustHeadValue(ds datas.Dataset) types.Value {
	val, ok, err := ds.MaybeHeadValue()
	d.PanicIfError(err)

	if !ok {
		panic("no head")
	}

	return val
}

func (s *nomsLogTestSuite) TestNArg() {
	dsName := "nArgTest"

	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("nbs", s.DBDir))
	s.NoError(err)
	defer sp.Close()

	db := sp.GetDatabase(context.Background())
	vrw := sp.GetVRW(context.Background())
	ds, err := db.GetDataset(context.Background(), dsName)
	s.NoError(err)

	ds, err = addCommit(ds, "1")
	s.NoError(err)
	h1, err := mustHead(ds).Hash(vrw.Format())
	s.NoError(err)
	ds, err = addCommit(ds, "2")
	s.NoError(err)
	h2, err := mustHead(ds).Hash(vrw.Format())
	s.NoError(err)
	ds, err = addCommit(ds, "3")
	s.NoError(err)
	h3, err := mustHead(ds).Hash(vrw.Format())
	s.NoError(err)

	dsSpec := spec.CreateValueSpecString("nbs", s.DBDir, dsName)
	res, _ := s.MustRun(main, []string{"log", "-n1", dsSpec})
	s.NotContains(res, h1.String())
	res, _ = s.MustRun(main, []string{"log", "-n0", dsSpec})
	s.Contains(res, h3.String())
	s.Contains(res, h2.String())
	s.Contains(res, h1.String())

	vSpec := spec.CreateValueSpecString("nbs", s.DBDir, "#"+h3.String())
	res, _ = s.MustRun(main, []string{"log", "-n1", vSpec})
	s.NotContains(res, h1.String())
	res, _ = s.MustRun(main, []string{"log", "-n0", vSpec})
	s.Contains(res, h3.String())
	s.Contains(res, h2.String())
	s.Contains(res, h1.String())
}

func (s *nomsLogTestSuite) TestEmptyCommit() {
	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("nbs", s.DBDir))
	s.NoError(err)
	defer sp.Close()

	db := sp.GetDatabase(context.Background())
	ds, err := db.GetDataset(context.Background(), "ds1")

	s.NoError(err)

	ds, err = db.Commit(context.Background(), ds, types.String("1"), datas.CommitOptions{Meta: &datas.CommitMeta{Name: "Yoo Hoo"}})
	s.NoError(err)

	ds, err = db.Commit(context.Background(), ds, types.String("2"), datas.CommitOptions{})
	s.NoError(err)

	dsSpec := spec.CreateValueSpecString("nbs", s.DBDir, "ds1")
	res, _ := s.MustRun(main, []string{"log", dsSpec})
	test.EqualsIgnoreHashes(s.T(), metaRes1, res)

	res, _ = s.MustRun(main, []string{"log", "--oneline", dsSpec})
	test.EqualsIgnoreHashes(s.T(), metaRes2, res)
}

func (s *nomsLogTestSuite) TestTruncation() {
	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("nbs", s.DBDir))
	s.NoError(err)
	defer sp.Close()
	db := sp.GetDatabase(context.Background())
	vrw := sp.GetVRW(context.Background())

	toNomsList := func(l []string) types.List {
		nv := []types.Value{}
		for _, v := range l {
			nv = append(nv, types.String(v))
		}

		lst, err := types.NewList(context.Background(), vrw, nv...)
		s.NoError(err)

		return lst
	}

	t, err := db.GetDataset(context.Background(), "truncate")
	s.NoError(err)

	t, err = addCommit(t, "the first line")
	s.NoError(err)

	l := []string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven"}
	_, err = addCommitWithValue(t, toNomsList(l))
	s.NoError(err)

	dsSpec := spec.CreateValueSpecString("nbs", s.DBDir, "truncate")
	res, _ := s.MustRun(main, []string{"log", "--graph", "--show-value", dsSpec})
	test.EqualsIgnoreHashes(s.T(), truncRes1, res)
	res, _ = s.MustRun(main, []string{"log", "--graph", dsSpec})
	test.EqualsIgnoreHashes(s.T(), diffTrunc1, res)

	res, _ = s.MustRun(main, []string{"log", "--graph", "--show-value", "--max-lines=-1", dsSpec})
	test.EqualsIgnoreHashes(s.T(), truncRes2, res)
	res, _ = s.MustRun(main, []string{"log", "--graph", "--max-lines=-1", dsSpec})
	test.EqualsIgnoreHashes(s.T(), diffTrunc2, res)

	res, _ = s.MustRun(main, []string{"log", "--graph", "--show-value", "--max-lines=0", dsSpec})
	test.EqualsIgnoreHashes(s.T(), truncRes3, res)
	res, _ = s.MustRun(main, []string{"log", "--graph", "--max-lines=0", dsSpec})
	test.EqualsIgnoreHashes(s.T(), diffTrunc3, res)
}

func TestBranchlistSplice(t *testing.T) {
	assert := assert.New(t)
	bl := branchList{}
	for i := 0; i < 4; i++ {
		bl = bl.Splice(0, 0, branch{})
	}
	assert.Equal(4, len(bl))
	bl = bl.Splice(3, 1)
	bl = bl.Splice(0, 1)
	bl = bl.Splice(1, 1)
	bl = bl.Splice(0, 1)
	assert.Zero(len(bl))

	for i := 0; i < 4; i++ {
		bl = bl.Splice(0, 0, branch{})
	}
	assert.Equal(4, len(bl))

	branchesToDelete := []int{1, 2, 3}
	bl = bl.RemoveBranches(branchesToDelete)
	assert.Equal(1, len(bl))
}

const (
	truncRes1  = "* 0agie9lniae8pdkmsmpjuio1idcuju43\n| Parent:         ts2338fiscn46jret32unhk5i82f80ub\n| Desc:           \"\"\n| Email:          \"\"\n| Name:           \"\"\n| Timestamp:      0\n| User_timestamp: 0\n| [  // 11 items\n|   \"one\",\n|   \"two\",\n| ...\n| \n* ts2338fiscn46jret32unhk5i82f80ub\n| Parent:         None\n| Desc:           \"\"\n| Email:          \"\"\n| Name:           \"\"\n| Timestamp:      0\n| User_timestamp: 0\n| \"the first line\"\n"
	diffTrunc1 = "* 0agie9lniae8pdkmsmpjuio1idcuju43\n| Parent:         ts2338fiscn46jret32unhk5i82f80ub\n| Desc:           \"\"\n| Email:          \"\"\n| Name:           \"\"\n| Timestamp:      0\n| User_timestamp: 0\n| -   \"the first line\"\n| +   [  // 11 items\n| +     \"one\",\n| ...\n| \n* ts2338fiscn46jret32unhk5i82f80ub\n| Parent:         None\n| Desc:           \"\"\n| Email:          \"\"\n| Name:           \"\"\n| Timestamp:      0\n| User_timestamp: 0\n| \n"

	truncRes2  = "* 0agie9lniae8pdkmsmpjuio1idcuju43\n| Parent:         ts2338fiscn46jret32unhk5i82f80ub\n| Desc:           \"\"\n| Email:          \"\"\n| Name:           \"\"\n| Timestamp:      0\n| User_timestamp: 0\n| [  // 11 items\n|   \"one\",\n|   \"two\",\n|   \"three\",\n|   \"four\",\n|   \"five\",\n|   \"six\",\n|   \"seven\",\n|   \"eight\",\n|   \"nine\",\n|   \"ten\",\n|   \"eleven\",\n| ]\n| \n* ts2338fiscn46jret32unhk5i82f80ub\n| Parent:         None\n| Desc:           \"\"\n| Email:          \"\"\n| Name:           \"\"\n| Timestamp:      0\n| User_timestamp: 0\n| \"the first line\"\n"
	diffTrunc2 = "* 0agie9lniae8pdkmsmpjuio1idcuju43\n| Parent:         ts2338fiscn46jret32unhk5i82f80ub\n| Desc:           \"\"\n| Email:          \"\"\n| Name:           \"\"\n| Timestamp:      0\n| User_timestamp: 0\n| -   \"the first line\"\n| +   [  // 11 items\n| +     \"one\",\n| +     \"two\",\n| +     \"three\",\n| +     \"four\",\n| +     \"five\",\n| +     \"six\",\n| +     \"seven\",\n| +     \"eight\",\n| +     \"nine\",\n| +     \"ten\",\n| +     \"eleven\",\n| +   ]\n| \n* ts2338fiscn46jret32unhk5i82f80ub\n| Parent:         None\n| Desc:           \"\"\n| Email:          \"\"\n| Name:           \"\"\n| Timestamp:      0\n| User_timestamp: 0\n| \n"

	truncRes3  = "* 0agie9lniae8pdkmsmpjuio1idcuju43\n| Parent:         ts2338fiscn46jret32unhk5i82f80ub\n* ts2338fiscn46jret32unhk5i82f80ub\n| Parent:         None\n"
	diffTrunc3 = "* 0agie9lniae8pdkmsmpjuio1idcuju43\n| Parent:         ts2338fiscn46jret32unhk5i82f80ub\n* ts2338fiscn46jret32unhk5i82f80ub\n| Parent:         None\n"

	metaRes1 = "m8tkea05tt7je6nfhe6i6nobmol501mr\nParent:         k8d1ap7m1tjqiulsr7g7pctmia4bmht8\nDesc:           \"\"\nEmail:          \"\"\nName:           \"\"\nTimestamp:      0\nUser_timestamp: 0\n-   \"1\"\n+   \"2\"\n\nk8d1ap7m1tjqiulsr7g7pctmia4bmht8\nParent:         None\nDesc:           \"\"\nEmail:          \"\"\nName:           \"Yoo Hoo\"\nTimestamp:      0\nUser_timestamp: 0\n\n"
	metaRes2 = "p7jmuh67vhfccnqk1bilnlovnms1m67o (Parent: f8gjiv5974ojir9tnrl2k393o4s1tf0r)\nf8gjiv5974ojir9tnrl2k393o4s1tf0r (Parent: None)\n"

	pathValue = "19no3lvc6t31q2vvsrd1462nf1shhc22\nParent:         ll6e085519dqqf9ijbp604hndr9s8miv\nDesc:           \"\"\nEmail:          \"\"\nName:           \"\"\nTimestamp:      0\nUser_timestamp: 0\n2\n\nll6e085519dqqf9ijbp604hndr9s8miv\nParent:         lc0ia0agtc7o009ams3b3mqsejtquns4\nDesc:           \"\"\nEmail:          \"\"\nName:           \"\"\nTimestamp:      0\nUser_timestamp: 0\n1\n\nlc0ia0agtc7o009ams3b3mqsejtquns4\nParent:         uce1bno19e77cvgr078irik4j9gs7pir\nDesc:           \"\"\nEmail:          \"\"\nName:           \"\"\nTimestamp:      0\nUser_timestamp: 0\n0\n\nuce1bno19e77cvgr078irik4j9gs7pir\nParent:         33sn2qmgs936f0r3ib3bb392glj1o1ap\nDesc:           \"\"\nEmail:          \"\"\nName:           \"\"\nTimestamp:      0\nUser_timestamp: 0\n<nil>\n33sn2qmgs936f0r3ib3bb392glj1o1ap\nParent:         None\nDesc:           \"\"\nEmail:          \"\"\nName:           \"\"\nTimestamp:      0\nUser_timestamp: 0\n<nil>\n"

	pathDiff = "19no3lvc6t31q2vvsrd1462nf1shhc22\nParent:         ll6e085519dqqf9ijbp604hndr9s8miv\nDesc:           \"\"\nEmail:          \"\"\nName:           \"\"\nTimestamp:      0\nUser_timestamp: 0\n-   1\n+   2\n\nll6e085519dqqf9ijbp604hndr9s8miv\nParent:         lc0ia0agtc7o009ams3b3mqsejtquns4\nDesc:           \"\"\nEmail:          \"\"\nName:           \"\"\nTimestamp:      0\nUser_timestamp: 0\n-   0\n+   1\n\nlc0ia0agtc7o009ams3b3mqsejtquns4\nParent:         uce1bno19e77cvgr078irik4j9gs7pir\nDesc:           \"\"\nEmail:          \"\"\nName:           \"\"\nTimestamp:      0\nUser_timestamp: 0\nold (#uce1bno19e77cvgr078irik4j9gs7pir.value.bar) not found\n\nuce1bno19e77cvgr078irik4j9gs7pir\nParent:         33sn2qmgs936f0r3ib3bb392glj1o1ap\nDesc:           \"\"\nEmail:          \"\"\nName:           \"\"\nTimestamp:      0\nUser_timestamp: 0\nnew (#uce1bno19e77cvgr078irik4j9gs7pir.value.bar) not found\nold (#33sn2qmgs936f0r3ib3bb392glj1o1ap.value.bar) not found\n\n33sn2qmgs936f0r3ib3bb392glj1o1ap\nParent:         None\nDesc:           \"\"\nEmail:          \"\"\nName:           \"\"\nTimestamp:      0\nUser_timestamp: 0\n\n"
)
