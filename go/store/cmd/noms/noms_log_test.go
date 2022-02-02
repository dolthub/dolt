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
	p, err := types.NewList(context.Background(), vrw, mustHeadRef(parentDs))

	if err != nil {
		return datas.Dataset{}, err
	}

	return newDs.Database().Commit(context.Background(), newDs, types.String(v), datas.CommitOptions{ParentsList: p})
}

func mergeDatasets(vrw types.ValueReadWriter, ds1, ds2 datas.Dataset, v string) (datas.Dataset, error) {
	p, err := types.NewList(context.Background(), vrw, mustHeadRef(ds1), mustHeadRef(ds2))

	if err != nil {
		return datas.Dataset{}, err
	}

	return ds1.Database().Commit(context.Background(), ds1, types.String(v), datas.CommitOptions{ParentsList: p})
}

func mustHead(ds datas.Dataset) types.Struct {
	s, ok := ds.MaybeHead()
	if !ok {
		panic("no head")
	}

	return s
}

func mustHeadRef(ds datas.Dataset) types.Ref {
	hr, ok, err := ds.MaybeHeadRef()
	d.PanicIfError(err)

	if !ok {
		panic("no head")
	}

	return hr
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
	vrw := sp.GetVRW(context.Background())
	ds, err := db.GetDataset(context.Background(), "ds1")

	s.NoError(err)

	meta, err := types.NewStruct(vrw.Format(), "Meta", map[string]types.Value{
		"longNameForTest": types.String("Yoo"),
		"test2":           types.String("Hoo"),
	})
	s.NoError(err)
	ds, err = db.Commit(context.Background(), ds, types.String("1"), datas.CommitOptions{Meta: meta})
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
	truncRes1  = "* p1442asfqnhgv1ebg6rijhl3kb9n4vt3\n| Parent: 4tq9si4tk8n0pead7hovehcbuued45sa\n| [  // 11 items\n|   \"one\",\n|   \"two\",\n|   \"three\",\n|   \"four\",\n|   \"five\",\n|   \"six\",\n|   \"seven\",\n| ...\n| \n* 4tq9si4tk8n0pead7hovehcbuued45sa\n| Parent: None\n| \"the first line\"\n"
	diffTrunc1 = "* p1442asfqnhgv1ebg6rijhl3kb9n4vt3\n| Parent: 4tq9si4tk8n0pead7hovehcbuued45sa\n| -   \"the first line\"\n| +   [  // 11 items\n| +     \"one\",\n| +     \"two\",\n| +     \"three\",\n| +     \"four\",\n| +     \"five\",\n| +     \"six\",\n| ...\n| \n* 4tq9si4tk8n0pead7hovehcbuued45sa\n| Parent: None\n| \n"

	truncRes2  = "* p1442asfqnhgv1ebg6rijhl3kb9n4vt3\n| Parent: 4tq9si4tk8n0pead7hovehcbuued45sa\n| [  // 11 items\n|   \"one\",\n|   \"two\",\n|   \"three\",\n|   \"four\",\n|   \"five\",\n|   \"six\",\n|   \"seven\",\n|   \"eight\",\n|   \"nine\",\n|   \"ten\",\n|   \"eleven\",\n| ]\n| \n* 4tq9si4tk8n0pead7hovehcbuued45sa\n| Parent: None\n| \"the first line\"\n"
	diffTrunc2 = "* p1442asfqnhgv1ebg6rijhl3kb9n4vt3\n| Parent: 4tq9si4tk8n0pead7hovehcbuued45sa\n| -   \"the first line\"\n| +   [  // 11 items\n| +     \"one\",\n| +     \"two\",\n| +     \"three\",\n| +     \"four\",\n| +     \"five\",\n| +     \"six\",\n| +     \"seven\",\n| +     \"eight\",\n| +     \"nine\",\n| +     \"ten\",\n| +     \"eleven\",\n| +   ]\n| \n* 4tq9si4tk8n0pead7hovehcbuued45sa\n| Parent: None\n| \n"

	truncRes3  = "* p1442asfqnhgv1ebg6rijhl3kb9n4vt3\n| Parent: 4tq9si4tk8n0pead7hovehcbuued45sa\n* 4tq9si4tk8n0pead7hovehcbuued45sa\n| Parent: None\n"
	diffTrunc3 = "* p1442asfqnhgv1ebg6rijhl3kb9n4vt3\n| Parent: 4tq9si4tk8n0pead7hovehcbuued45sa\n* 4tq9si4tk8n0pead7hovehcbuued45sa\n| Parent: None\n"

	metaRes1 = "p7jmuh67vhfccnqk1bilnlovnms1m67o\nParent: f8gjiv5974ojir9tnrl2k393o4s1tf0r\n-   \"1\"\n+   \"2\"\n\nf8gjiv5974ojir9tnrl2k393o4s1tf0r\nParent:          None\nLongNameForTest: \"Yoo\"\nTest2:           \"Hoo\"\n\n"
	metaRes2 = "p7jmuh67vhfccnqk1bilnlovnms1m67o (Parent: f8gjiv5974ojir9tnrl2k393o4s1tf0r)\nf8gjiv5974ojir9tnrl2k393o4s1tf0r (Parent: None)\n"

	pathValue = "oki4cv7vkh743rccese3r3omf6l6mao4\nParent: lca4vejkm0iqsk7ok5322pt61u4otn6q\n2\n\nlca4vejkm0iqsk7ok5322pt61u4otn6q\nParent: u42pi8ukgkvpoi6n7d46cklske41oguf\n1\n\nu42pi8ukgkvpoi6n7d46cklske41oguf\nParent: hgmlqmsnrb3sp9jqc6mas8kusa1trrs2\n0\n\nhgmlqmsnrb3sp9jqc6mas8kusa1trrs2\nParent: hffiuecdpoq622tamm3nvungeca99ohl\n<nil>\nhffiuecdpoq622tamm3nvungeca99ohl\nParent: None\n<nil>\n"

	pathDiff = "oki4cv7vkh743rccese3r3omf6l6mao4\nParent: lca4vejkm0iqsk7ok5322pt61u4otn6q\n-   1\n+   2\n\nlca4vejkm0iqsk7ok5322pt61u4otn6q\nParent: u42pi8ukgkvpoi6n7d46cklske41oguf\n-   0\n+   1\n\nu42pi8ukgkvpoi6n7d46cklske41oguf\nParent: hgmlqmsnrb3sp9jqc6mas8kusa1trrs2\nold (#hgmlqmsnrb3sp9jqc6mas8kusa1trrs2.value.bar) not found\n\nhgmlqmsnrb3sp9jqc6mas8kusa1trrs2\nParent: hffiuecdpoq622tamm3nvungeca99ohl\nnew (#hgmlqmsnrb3sp9jqc6mas8kusa1trrs2.value.bar) not found\nold (#hffiuecdpoq622tamm3nvungeca99ohl.value.bar) not found\n\nhffiuecdpoq622tamm3nvungeca99ohl\nParent: None\n\n"
)
