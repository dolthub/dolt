// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"
)

type testExiter struct{}
type exitError struct {
	code int
}

func (e exitError) Error() string {
	return fmt.Sprintf("Exiting with code: %d", e.code)
}

func (testExiter) Exit(code int) {
	panic(exitError{code})
}

func TestNomsLog(t *testing.T) {
	d.UtilExiter = testExiter{}
	suite.Run(t, &nomsLogTestSuite{})
}

type nomsLogTestSuite struct {
	clienttest.ClientTestSuite
}

func testCommitInResults(s *nomsLogTestSuite, str string, i int) {
	ds, err := spec.GetDataset(str)
	s.NoError(err)
	ds, err = ds.CommitValue(types.Number(i))
	s.NoError(err)
	commit := ds.Head()
	ds.Database().Close()
	res, _ := s.Run(main, []string{"log", str})
	s.Contains(res, commit.Hash().String())
}

func (s *nomsLogTestSuite) TestNomsLog() {
	datasetName := "dsTest"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)
	ds, err := spec.GetDataset(str)
	s.NoError(err)

	ds.Database().Close()
	s.Panics(func() { s.Run(main, []string{"log", str}) })

	testCommitInResults(s, str, 1)
	testCommitInResults(s, str, 2)
}

func addCommit(ds dataset.Dataset, v string) (dataset.Dataset, error) {
	return ds.CommitValue(types.String(v))
}

func addCommitWithValue(ds dataset.Dataset, v types.Value) (dataset.Dataset, error) {
	return ds.CommitValue(v)
}

func addBranchedDataset(newDs, parentDs dataset.Dataset, v string) (dataset.Dataset, error) {
	p := types.NewSet(parentDs.HeadRef())
	return newDs.Commit(types.String(v), dataset.CommitOptions{Parents: p})
}

func mergeDatasets(ds1, ds2 dataset.Dataset, v string) (dataset.Dataset, error) {
	p := types.NewSet(ds1.HeadRef(), ds2.HeadRef())
	return ds1.Commit(types.String(v), dataset.CommitOptions{Parents: p})
}

func (s *nomsLogTestSuite) TestNArg() {
	str := spec.CreateDatabaseSpecString("ldb", s.LdbDir)
	dsName := "nArgTest"
	db, err := spec.GetDatabase(str)
	s.NoError(err)

	ds := dataset.NewDataset(db, dsName)

	ds, err = addCommit(ds, "1")
	h1 := ds.Head().Hash()
	s.NoError(err)
	ds, err = addCommit(ds, "2")
	s.NoError(err)
	h2 := ds.Head().Hash()
	ds, err = addCommit(ds, "3")
	s.NoError(err)
	h3 := ds.Head().Hash()
	db.Close()

	dsSpec := spec.CreateValueSpecString("ldb", s.LdbDir, dsName)
	res, _ := s.Run(main, []string{"log", "-n=1", dsSpec})
	s.NotContains(res, h1.String())
	res, _ = s.Run(main, []string{"log", "-n=0", dsSpec})
	s.Contains(res, h3.String())
	s.Contains(res, h2.String())
	s.Contains(res, h1.String())

	vSpec := spec.CreateValueSpecString("ldb", s.LdbDir, "#"+h3.String())
	res, _ = s.Run(main, []string{"log", "-n=1", vSpec})
	s.NotContains(res, h1.String())
	res, _ = s.Run(main, []string{"log", "-n=0", vSpec})
	s.Contains(res, h3.String())
	s.Contains(res, h2.String())
	s.Contains(res, h1.String())
}

func (s *nomsLogTestSuite) TestNomsGraph1() {
	str := spec.CreateDatabaseSpecString("ldb", s.LdbDir)
	db, err := spec.GetDatabase(str)
	s.NoError(err)

	b1 := dataset.NewDataset(db, "b1")

	b1, err = addCommit(b1, "1")
	s.NoError(err)
	b1, err = addCommit(b1, "2")
	s.NoError(err)
	b1, err = addCommit(b1, "3")
	s.NoError(err)

	b2 := dataset.NewDataset(db, "b2")
	b2, err = addBranchedDataset(b2, b1, "3.1")
	s.NoError(err)

	b1, err = addCommit(b1, "3.2")
	s.NoError(err)
	b1, err = addCommit(b1, "3.6")
	s.NoError(err)

	b3 := dataset.NewDataset(db, "b3")
	b3, err = addBranchedDataset(b3, b2, "3.1.3")
	s.NoError(err)
	b3, err = addCommit(b3, "3.1.5")
	s.NoError(err)
	b3, err = addCommit(b3, "3.1.7")
	s.NoError(err)

	b2, err = mergeDatasets(b2, b3, "3.5")
	s.NoError(err)
	b2, err = addCommit(b2, "3.7")
	s.NoError(err)

	b1, err = mergeDatasets(b1, b2, "4")
	s.NoError(err)

	b1, err = addCommit(b1, "5")
	s.NoError(err)
	b1, err = addCommit(b1, "6")
	s.NoError(err)
	b1, err = addCommit(b1, "7")
	s.NoError(err)

	b1.Database().Close()
	res, _ := s.Run(main, []string{"log", "-graph", "-show-value=true", spec.CreateValueSpecString("ldb", s.LdbDir, "b1")})
	s.Equal(graphRes1, res)
	res, _ = s.Run(main, []string{"log", "-graph", "-show-value=false", spec.CreateValueSpecString("ldb", s.LdbDir, "b1")})
	s.Equal(diffRes1, res)
}

func (s *nomsLogTestSuite) TestNomsGraph2() {
	str := spec.CreateDatabaseSpecString("ldb", s.LdbDir)
	db, err := spec.GetDatabase(str)
	s.NoError(err)

	ba := dataset.NewDataset(db, "ba")

	ba, err = addCommit(ba, "1")
	s.NoError(err)

	bb := dataset.NewDataset(db, "bb")
	bb, err = addCommit(bb, "10")
	s.NoError(err)

	bc := dataset.NewDataset(db, "bc")
	bc, err = addCommit(bc, "100")
	s.NoError(err)

	ba, err = mergeDatasets(ba, bb, "11")
	s.NoError(err)

	_, err = mergeDatasets(ba, bc, "101")
	s.NoError(err)

	db.Close()

	res, _ := s.Run(main, []string{"log", "-graph", "-show-value=true", spec.CreateValueSpecString("ldb", s.LdbDir, "ba")})
	s.Equal(graphRes2, res)
	res, _ = s.Run(main, []string{"log", "-graph", "-show-value=false", spec.CreateValueSpecString("ldb", s.LdbDir, "ba")})
	s.Equal(diffRes2, res)
}

func (s *nomsLogTestSuite) TestNomsGraph3() {
	str := spec.CreateDatabaseSpecString("ldb", s.LdbDir)
	db, err := spec.GetDatabase(str)
	s.NoError(err)

	w := dataset.NewDataset(db, "w")

	w, err = addCommit(w, "1")
	s.NoError(err)

	w, err = addCommit(w, "2")
	s.NoError(err)

	x := dataset.NewDataset(db, "x")
	x, err = addBranchedDataset(x, w, "20-x")
	s.NoError(err)

	y := dataset.NewDataset(db, "y")
	y, err = addBranchedDataset(y, w, "200-y")
	s.NoError(err)

	z := dataset.NewDataset(db, "z")
	z, err = addBranchedDataset(z, w, "2000-z")
	s.NoError(err)

	w, err = mergeDatasets(w, x, "22-wx")
	s.NoError(err)

	w, err = mergeDatasets(w, y, "222-wy")
	s.NoError(err)

	_, err = mergeDatasets(w, z, "2222-wz")
	s.NoError(err)

	db.Close()
	res, _ := s.Run(main, []string{"log", "-graph", "-show-value=true", spec.CreateValueSpecString("ldb", s.LdbDir, "w")})
	s.Equal(graphRes3, res)
	res, _ = s.Run(main, []string{"log", "-graph", "-show-value=false", spec.CreateValueSpecString("ldb", s.LdbDir, "w")})
	s.Equal(diffRes3, res)
}

func (s *nomsLogTestSuite) TestTruncation() {
	toNomsList := func(l []string) types.List {
		nv := []types.Value{}
		for _, v := range l {
			nv = append(nv, types.String(v))
		}
		return types.NewList(nv...)
	}

	str := spec.CreateDatabaseSpecString("ldb", s.LdbDir)
	db, err := spec.GetDatabase(str)
	s.NoError(err)

	t := dataset.NewDataset(db, "truncate")

	t, err = addCommit(t, "the first line")
	s.NoError(err)

	l := []string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven"}
	_, err = addCommitWithValue(t, toNomsList(l))
	s.NoError(err)
	db.Close()

	dsSpec := spec.CreateValueSpecString("ldb", s.LdbDir, "truncate")
	res, _ := s.Run(main, []string{"log", "-graph", "-show-value=true", dsSpec})
	s.Equal(truncRes1, res)
	res, _ = s.Run(main, []string{"log", "-graph", "-show-value=false", dsSpec})
	s.Equal(diffTrunc1, res)

	res, _ = s.Run(main, []string{"log", "-graph", "-show-value=true", "-max-lines=-1", dsSpec})
	s.Equal(truncRes2, res)
	res, _ = s.Run(main, []string{"log", "-graph", "-show-value=false", "-max-lines=-1", dsSpec})
	s.Equal(diffTrunc2, res)

	res, _ = s.Run(main, []string{"log", "-graph", "-show-value=true", "-max-lines=0", dsSpec})
	s.Equal(truncRes3, res)
	res, _ = s.Run(main, []string{"log", "-graph", "-show-value=false", "-max-lines=0", dsSpec})
	s.Equal(diffTrunc3, res)
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
	graphRes1 = "* 91h4s89cr5npidfk74v2i7pg0u467sj3\n| Parent: 4vtujeavsaea0g3rf8ceus0452a6qcpv\n| \"7\"\n| \n* 4vtujeavsaea0g3rf8ceus0452a6qcpv\n| Parent: gtd79j3i0cbmecqsavmpns0doub5uh48\n| \"6\"\n| \n* gtd79j3i0cbmecqsavmpns0doub5uh48\n| Parent: hmilshma5m5tghrmeqsl5guvhq4jrj1p\n| \"5\"\n| \n*   hmilshma5m5tghrmeqsl5guvhq4jrj1p\n|\\  Merge: b5uu96npfn8hns3djrr06uf07rsre5jl p70ra0af223q7082hrccmmq52adlq5qc\n| | \"4\"\n| | \n* | b5uu96npfn8hns3djrr06uf07rsre5jl\n| | Parent: 3qnjukjdf6bi6dhech80p1mp44lqv8t6\n| | \"3.7\"\n| | \n* |   3qnjukjdf6bi6dhech80p1mp44lqv8t6\n|\\ \\  Merge: cfq2u3akjrjj2mg00h22miq85ukdovho 28oue1i24hr3vvhfh9ppjqpv2lk8ou7c\n| | | \"3.5\"\n| | | \n| * | 28oue1i24hr3vvhfh9ppjqpv2lk8ou7c\n| | | Parent: 3o1fpog1r9kflm9m47qk0fc8j5kjnggm\n| | | \"3.1.7\"\n| | | \n| * | 3o1fpog1r9kflm9m47qk0fc8j5kjnggm\n| | | Parent: rt9m1mqbstb21h6r3gjnd70lt7eat2jd\n| | | \"3.1.5\"\n| | | \n| * | rt9m1mqbstb21h6r3gjnd70lt7eat2jd\n| | | Parent: cfq2u3akjrjj2mg00h22miq85ukdovho\n| | | \"3.1.3\"\n| | | \n| | * p70ra0af223q7082hrccmmq52adlq5qc\n|/  | Parent: s85k2cs8rkhs1lk8kvdcr1g66doe1ot1\n|   | \"3.6\"\n|   | \n* | cfq2u3akjrjj2mg00h22miq85ukdovho\n| | Parent: i0do4efohj7679fr9pe4v79281l66nf3\n| | \"3.1\"\n| | \n| * s85k2cs8rkhs1lk8kvdcr1g66doe1ot1\n|/  Parent: i0do4efohj7679fr9pe4v79281l66nf3\n|   \"3.2\"\n|   \n* i0do4efohj7679fr9pe4v79281l66nf3\n| Parent: qjslbtifp9ueasdh50p989p31u5dqgl9\n| \"3\"\n| \n* qjslbtifp9ueasdh50p989p31u5dqgl9\n| Parent: hjrlok9ts52founodvoj8sn9nr0cln2a\n| \"2\"\n| \n* hjrlok9ts52founodvoj8sn9nr0cln2a\n| Parent: None\n| \"1\"\n"
	diffRes1  = "* 91h4s89cr5npidfk74v2i7pg0u467sj3\n| Parent: 4vtujeavsaea0g3rf8ceus0452a6qcpv\n| -   \"6\"\n| +   \"7\"\n| \n* 4vtujeavsaea0g3rf8ceus0452a6qcpv\n| Parent: gtd79j3i0cbmecqsavmpns0doub5uh48\n| -   \"5\"\n| +   \"6\"\n| \n* gtd79j3i0cbmecqsavmpns0doub5uh48\n| Parent: hmilshma5m5tghrmeqsl5guvhq4jrj1p\n| -   \"4\"\n| +   \"5\"\n| \n*   hmilshma5m5tghrmeqsl5guvhq4jrj1p\n|\\  Merge: b5uu96npfn8hns3djrr06uf07rsre5jl p70ra0af223q7082hrccmmq52adlq5qc\n| | -   \"3.7\"\n| | +   \"4\"\n| | \n* | b5uu96npfn8hns3djrr06uf07rsre5jl\n| | Parent: 3qnjukjdf6bi6dhech80p1mp44lqv8t6\n| | -   \"3.5\"\n| | +   \"3.7\"\n| | \n* |   3qnjukjdf6bi6dhech80p1mp44lqv8t6\n|\\ \\  Merge: cfq2u3akjrjj2mg00h22miq85ukdovho 28oue1i24hr3vvhfh9ppjqpv2lk8ou7c\n| | | -   \"3.1\"\n| | | +   \"3.5\"\n| | | \n| * | 28oue1i24hr3vvhfh9ppjqpv2lk8ou7c\n| | | Parent: 3o1fpog1r9kflm9m47qk0fc8j5kjnggm\n| | | -   \"3.1.5\"\n| | | +   \"3.1.7\"\n| | | \n| * | 3o1fpog1r9kflm9m47qk0fc8j5kjnggm\n| | | Parent: rt9m1mqbstb21h6r3gjnd70lt7eat2jd\n| | | -   \"3.1.3\"\n| | | +   \"3.1.5\"\n| | | \n| * | rt9m1mqbstb21h6r3gjnd70lt7eat2jd\n| | | Parent: cfq2u3akjrjj2mg00h22miq85ukdovho\n| | | -   \"3.1\"\n| | | +   \"3.1.3\"\n| | | \n| | * p70ra0af223q7082hrccmmq52adlq5qc\n|/  | Parent: s85k2cs8rkhs1lk8kvdcr1g66doe1ot1\n|   | -   \"3.2\"\n|   | +   \"3.6\"\n|   | \n* | cfq2u3akjrjj2mg00h22miq85ukdovho\n| | Parent: i0do4efohj7679fr9pe4v79281l66nf3\n| | -   \"3\"\n| | +   \"3.1\"\n| | \n| * s85k2cs8rkhs1lk8kvdcr1g66doe1ot1\n|/  Parent: i0do4efohj7679fr9pe4v79281l66nf3\n|   -   \"3\"\n|   +   \"3.2\"\n|   \n* i0do4efohj7679fr9pe4v79281l66nf3\n| Parent: qjslbtifp9ueasdh50p989p31u5dqgl9\n| -   \"2\"\n| +   \"3\"\n| \n* qjslbtifp9ueasdh50p989p31u5dqgl9\n| Parent: hjrlok9ts52founodvoj8sn9nr0cln2a\n| -   \"1\"\n| +   \"2\"\n| \n* hjrlok9ts52founodvoj8sn9nr0cln2a\n| Parent: None\n| \n"

	graphRes2 = "*   t4hqesdg07nhc0aifdd3upm1qm764hj3\n|\\  Merge: d605af3j18dfo1tvqq7ng6kg9kbpa83t c02vftjngvm9rjkrtkl3q7pu6i1j183h\n| | \"101\"\n| | \n* |   d605af3j18dfo1tvqq7ng6kg9kbpa83t\n|\\ \\  Merge: hjrlok9ts52founodvoj8sn9nr0cln2a peq91jhoui1epom2bqtnks02s3oqqcqa\n| | | \"11\"\n| | | \n* | hjrlok9ts52founodvoj8sn9nr0cln2a\n| | Parent: None\n| | \"1\"\n| | \n* peq91jhoui1epom2bqtnks02s3oqqcqa\n| Parent: None\n| \"10\"\n| \n* c02vftjngvm9rjkrtkl3q7pu6i1j183h\n| Parent: None\n| \"100\"\n"
	diffRes2  = "*   t4hqesdg07nhc0aifdd3upm1qm764hj3\n|\\  Merge: d605af3j18dfo1tvqq7ng6kg9kbpa83t c02vftjngvm9rjkrtkl3q7pu6i1j183h\n| | -   \"11\"\n| | +   \"101\"\n| | \n* |   d605af3j18dfo1tvqq7ng6kg9kbpa83t\n|\\ \\  Merge: hjrlok9ts52founodvoj8sn9nr0cln2a peq91jhoui1epom2bqtnks02s3oqqcqa\n| | | -   \"1\"\n| | | +   \"11\"\n| | | \n* | hjrlok9ts52founodvoj8sn9nr0cln2a\n| | Parent: None\n| | \n* peq91jhoui1epom2bqtnks02s3oqqcqa\n| Parent: None\n| \n* c02vftjngvm9rjkrtkl3q7pu6i1j183h\n| Parent: None\n| \n"

	graphRes3 = "*   qgjpni6tolcadp9pav77lrbrf3cqbp0p\n|\\  Merge: tagot7fan400kdk9sb9rta21dgq1hpa1 81q75vat1jgrr2chnnn3vv3v42g3bli4\n| | \"2222-wz\"\n| | \n| *   81q75vat1jgrr2chnnn3vv3v42g3bli4\n| |\\  Merge: p77qd1jgnkb1gi8ra44vpg4hj4pd0ql5 6c9tlms349lkv7qdvksbcqd55lf87nrh\n| | | \"222-wy\"\n| | | \n| * |   p77qd1jgnkb1gi8ra44vpg4hj4pd0ql5\n| |\\ \\  Merge: qjslbtifp9ueasdh50p989p31u5dqgl9 gcv2e7tj9qqncbog7uobddr3r8mr7flb\n| | | | \"22-wx\"\n| | | | \n* | | | tagot7fan400kdk9sb9rta21dgq1hpa1\n| | | | Parent: qjslbtifp9ueasdh50p989p31u5dqgl9\n| | | | \"2000-z\"\n| | | | \n| | * | gcv2e7tj9qqncbog7uobddr3r8mr7flb\n| | | | Parent: qjslbtifp9ueasdh50p989p31u5dqgl9\n| | | | \"20-x\"\n| | | | \n| | | * 6c9tlms349lkv7qdvksbcqd55lf87nrh\n|/ / /  Parent: qjslbtifp9ueasdh50p989p31u5dqgl9\n|       \"200-y\"\n|       \n* qjslbtifp9ueasdh50p989p31u5dqgl9\n| Parent: hjrlok9ts52founodvoj8sn9nr0cln2a\n| \"2\"\n| \n* hjrlok9ts52founodvoj8sn9nr0cln2a\n| Parent: None\n| \"1\"\n"
	diffRes3  = "*   qgjpni6tolcadp9pav77lrbrf3cqbp0p\n|\\  Merge: tagot7fan400kdk9sb9rta21dgq1hpa1 81q75vat1jgrr2chnnn3vv3v42g3bli4\n| | -   \"2000-z\"\n| | +   \"2222-wz\"\n| | \n| *   81q75vat1jgrr2chnnn3vv3v42g3bli4\n| |\\  Merge: p77qd1jgnkb1gi8ra44vpg4hj4pd0ql5 6c9tlms349lkv7qdvksbcqd55lf87nrh\n| | | -   \"22-wx\"\n| | | +   \"222-wy\"\n| | | \n| * |   p77qd1jgnkb1gi8ra44vpg4hj4pd0ql5\n| |\\ \\  Merge: qjslbtifp9ueasdh50p989p31u5dqgl9 gcv2e7tj9qqncbog7uobddr3r8mr7flb\n| | | | -   \"2\"\n| | | | +   \"22-wx\"\n| | | | \n* | | | tagot7fan400kdk9sb9rta21dgq1hpa1\n| | | | Parent: qjslbtifp9ueasdh50p989p31u5dqgl9\n| | | | -   \"2\"\n| | | | +   \"2000-z\"\n| | | | \n| | * | gcv2e7tj9qqncbog7uobddr3r8mr7flb\n| | | | Parent: qjslbtifp9ueasdh50p989p31u5dqgl9\n| | | | -   \"2\"\n| | | | +   \"20-x\"\n| | | | \n| | | * 6c9tlms349lkv7qdvksbcqd55lf87nrh\n|/ / /  Parent: qjslbtifp9ueasdh50p989p31u5dqgl9\n|       -   \"2\"\n|       +   \"200-y\"\n|       \n* qjslbtifp9ueasdh50p989p31u5dqgl9\n| Parent: hjrlok9ts52founodvoj8sn9nr0cln2a\n| -   \"1\"\n| +   \"2\"\n| \n* hjrlok9ts52founodvoj8sn9nr0cln2a\n| Parent: None\n| \n"

	truncRes1  = "* 4tig337dohlmek0n2o3iah9qd76ac5cd\n| Parent: is6oujms9lhrnf4uc39um6kd7ej12h1f\n| List<String>([  // 11 items\n|   \"one\",\n|   \"two\",\n|   \"three\",\n|   \"four\",\n|   \"five\",\n|   \"six\",\n|   \"seven\",\n| ...\n| \n* is6oujms9lhrnf4uc39um6kd7ej12h1f\n| Parent: None\n| \"the first line\"\n"
	diffTrunc1 = "* 4tig337dohlmek0n2o3iah9qd76ac5cd\n| Parent: is6oujms9lhrnf4uc39um6kd7ej12h1f\n| -   \"the first line\"\n| +   [  // 11 items\n| +     \"one\",\n| +     \"two\",\n| +     \"three\",\n| +     \"four\",\n| +     \"five\",\n| +     \"six\",\n| ...\n| \n* is6oujms9lhrnf4uc39um6kd7ej12h1f\n| Parent: None\n| \n"

	truncRes2  = "* 4tig337dohlmek0n2o3iah9qd76ac5cd\n| Parent: is6oujms9lhrnf4uc39um6kd7ej12h1f\n| List<String>([  // 11 items\n|   \"one\",\n|   \"two\",\n|   \"three\",\n|   \"four\",\n|   \"five\",\n|   \"six\",\n|   \"seven\",\n|   \"eight\",\n|   \"nine\",\n|   \"ten\",\n|   \"eleven\",\n| ])\n| \n* is6oujms9lhrnf4uc39um6kd7ej12h1f\n| Parent: None\n| \"the first line\"\n"
	diffTrunc2 = "* 4tig337dohlmek0n2o3iah9qd76ac5cd\n| Parent: is6oujms9lhrnf4uc39um6kd7ej12h1f\n| -   \"the first line\"\n| +   [  // 11 items\n| +     \"one\",\n| +     \"two\",\n| +     \"three\",\n| +     \"four\",\n| +     \"five\",\n| +     \"six\",\n| +     \"seven\",\n| +     \"eight\",\n| +     \"nine\",\n| +     \"ten\",\n| +     \"eleven\",\n| +   ]\n| \n* is6oujms9lhrnf4uc39um6kd7ej12h1f\n| Parent: None\n| \n"

	truncRes3  = "* 4tig337dohlmek0n2o3iah9qd76ac5cd\n| Parent: is6oujms9lhrnf4uc39um6kd7ej12h1f\n* is6oujms9lhrnf4uc39um6kd7ej12h1f\n| Parent: None\n"
	diffTrunc3 = "* 4tig337dohlmek0n2o3iah9qd76ac5cd\n| Parent: is6oujms9lhrnf4uc39um6kd7ej12h1f\n* is6oujms9lhrnf4uc39um6kd7ej12h1f\n| Parent: None\n"
)
