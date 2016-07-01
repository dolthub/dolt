// Copyright 2016 The Noms Authors. All rights reserved.
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
	"github.com/attic-labs/noms/samples/go/test_util"
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

func TestNomsShow(t *testing.T) {
	d.UtilExiter = testExiter{}
	suite.Run(t, &nomsShowTestSuite{})
}

type nomsShowTestSuite struct {
	test_util.ClientTestSuite
}

func testCommitInResults(s *nomsShowTestSuite, str string, i int) {
	ds, err := spec.GetDataset(str)
	s.NoError(err)
	ds, err = ds.Commit(types.Number(i))
	s.NoError(err)
	commit := ds.Head()
	ds.Database().Close()
	s.Contains(s.Run(main, []string{str}), commit.Hash().String())
}

func (s *nomsShowTestSuite) TestNomsLog() {
	datasetName := "dsTest"
	str := test_util.CreateValueSpecString("ldb", s.LdbDir, datasetName)
	ds, err := spec.GetDataset(str)
	s.NoError(err)

	ds.Database().Close()
	s.Panics(func() { s.Run(main, []string{str}) })

	testCommitInResults(s, str, 1)
	testCommitInResults(s, str, 2)
}

func addCommit(ds dataset.Dataset, v string) (dataset.Dataset, error) {
	return ds.Commit(types.String(v))
}

func addCommitWithValue(ds dataset.Dataset, v types.Value) (dataset.Dataset, error) {
	return ds.Commit(v)
}

func addBranchedDataset(newDs, parentDs dataset.Dataset, v string) (dataset.Dataset, error) {
	return newDs.CommitWithParents(types.String(v), types.NewSet().Insert(parentDs.HeadRef()))
}

func mergeDatasets(ds1, ds2 dataset.Dataset, v string) (dataset.Dataset, error) {
	return ds1.CommitWithParents(types.String(v), types.NewSet(ds1.HeadRef(), ds2.HeadRef()))
}

func (s *nomsShowTestSuite) TestNArg() {
	str := test_util.CreateDatabaseSpecString("ldb", s.LdbDir)
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

	dsSpec := test_util.CreateValueSpecString("ldb", s.LdbDir, dsName)
	s.NotContains(s.Run(main, []string{"-n=1", dsSpec}), h1.String())
	res := s.Run(main, []string{"-n=0", dsSpec})
	s.Contains(res, h3.String())
	s.Contains(res, h2.String())
	s.Contains(res, h1.String())

	vSpec := test_util.CreateValueSpecString("ldb", s.LdbDir, "#"+h3.String())
	s.NotContains(s.Run(main, []string{"-n=1", vSpec}), h1.String())
	res = s.Run(main, []string{"-n=0", vSpec})
	s.Contains(res, h3.String())
	s.Contains(res, h2.String())
	s.Contains(res, h1.String())
}

func (s *nomsShowTestSuite) TestNomsGraph1() {
	str := test_util.CreateDatabaseSpecString("ldb", s.LdbDir)
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
	s.Equal(graphRes1, s.Run(main, []string{"-graph", "-show-value=true", test_util.CreateValueSpecString("ldb", s.LdbDir, "b1")}))
	s.Equal(diffRes1, s.Run(main, []string{"-graph", "-show-value=false", test_util.CreateValueSpecString("ldb", s.LdbDir, "b1")}))
}

func (s *nomsShowTestSuite) TestNomsGraph2() {
	str := test_util.CreateDatabaseSpecString("ldb", s.LdbDir)
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
	s.Equal(graphRes2, s.Run(main, []string{"-graph", "-show-value=true", test_util.CreateValueSpecString("ldb", s.LdbDir, "ba")}))
	s.Equal(diffRes2, s.Run(main, []string{"-graph", "-show-value=false", test_util.CreateValueSpecString("ldb", s.LdbDir, "ba")}))
}

func (s *nomsShowTestSuite) TestNomsGraph3() {
	str := test_util.CreateDatabaseSpecString("ldb", s.LdbDir)
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
	s.Equal(graphRes3, s.Run(main, []string{"-graph", "-show-value=true", test_util.CreateValueSpecString("ldb", s.LdbDir, "w")}))
	s.Equal(diffRes3, s.Run(main, []string{"-graph", "-show-value=false", test_util.CreateValueSpecString("ldb", s.LdbDir, "w")}))
}

func (s *nomsShowTestSuite) TestTruncation() {
	toNomsList := func(l []string) types.List {
		nv := []types.Value{}
		for _, v := range l {
			nv = append(nv, types.String(v))
		}
		return types.NewList(nv...)
	}

	str := test_util.CreateDatabaseSpecString("ldb", s.LdbDir)
	db, err := spec.GetDatabase(str)
	s.NoError(err)

	t := dataset.NewDataset(db, "truncate")

	t, err = addCommit(t, "the first line")
	s.NoError(err)

	l := []string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven"}
	_, err = addCommitWithValue(t, toNomsList(l))
	s.NoError(err)
	db.Close()

	dsSpec := test_util.CreateValueSpecString("ldb", s.LdbDir, "truncate")
	s.Equal(truncRes1, s.Run(main, []string{"-graph", "-show-value=true", dsSpec}))
	s.Equal(diffTrunc1, s.Run(main, []string{"-graph", "-show-value=false", dsSpec}))

	s.Equal(truncRes2, s.Run(main, []string{"-graph", "-show-value=true", "-max-lines=-1", dsSpec}))
	s.Equal(diffTrunc2, s.Run(main, []string{"-graph", "-show-value=false", "-max-lines=-1", dsSpec}))

	s.Equal(truncRes3, s.Run(main, []string{"-graph", "-show-value=true", "-max-lines=0", dsSpec}))
	s.Equal(diffTrunc3, s.Run(main, []string{"-graph", "-show-value=false", "-max-lines=0", dsSpec}))
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
	graphRes1 = "* sha1-0b0a92f5515b5778194d07d8011b7c18bf9be178\n| Parent: sha1-14491727789ebd03ca0ddd5e3a1307ca3e2651dc\n| \"7\"\n| \n* sha1-14491727789ebd03ca0ddd5e3a1307ca3e2651dc\n| Parent: sha1-b5a8493d7be003673216bdc0ed275fbbacbb9b08\n| \"6\"\n| \n* sha1-b5a8493d7be003673216bdc0ed275fbbacbb9b08\n| Parent: sha1-2b837c2e2dd0e6ef015e4742bd969f9fa1641f38\n| \"5\"\n| \n*   sha1-2b837c2e2dd0e6ef015e4742bd969f9fa1641f38\n|\\  Merge: sha1-1f622d4006278a63cc77e95c6efcaf83f46606c4 sha1-eca65988a4ade585b8502d8f9fc37a5a5da94ab2\n| | \"4\"\n| | \n| * sha1-eca65988a4ade585b8502d8f9fc37a5a5da94ab2\n| | Parent: sha1-890cf3b1d49228ace9d691a627131a2872e42eb4\n| | \"3.7\"\n| | \n| *   sha1-890cf3b1d49228ace9d691a627131a2872e42eb4\n| |\\  Merge: sha1-4f72aeacef670bc6884a97de5a363b0674ccd008 sha1-fa78bedf601fc6d49ebf70865944dfccf5b5a133\n| | | \"3.5\"\n| | | \n| | * sha1-fa78bedf601fc6d49ebf70865944dfccf5b5a133\n| | | Parent: sha1-72f530e2ba8ba3168de8da8d15271cd856ac5c2b\n| | | \"3.1.7\"\n| | | \n| | * sha1-72f530e2ba8ba3168de8da8d15271cd856ac5c2b\n| | | Parent: sha1-dda04a898b2048558b4238a4f111364ff5c92ab8\n| | | \"3.1.5\"\n| | | \n* | | sha1-1f622d4006278a63cc77e95c6efcaf83f46606c4\n| | | Parent: sha1-e1583426c4201c7602b4aa1ae835915666d933f0\n| | | \"3.6\"\n| | | \n| | * sha1-dda04a898b2048558b4238a4f111364ff5c92ab8\n| | | Parent: sha1-4f72aeacef670bc6884a97de5a363b0674ccd008\n| | | \"3.1.3\"\n| | | \n* | | sha1-e1583426c4201c7602b4aa1ae835915666d933f0\n| |/  Parent: sha1-27290eda714ce5d47df05e8f77a0986647887e32\n| |   \"3.2\"\n| |   \n| * sha1-4f72aeacef670bc6884a97de5a363b0674ccd008\n|/  Parent: sha1-27290eda714ce5d47df05e8f77a0986647887e32\n|   \"3.1\"\n|   \n* sha1-27290eda714ce5d47df05e8f77a0986647887e32\n| Parent: sha1-624532f5d8a5839344ccfc465957a975e1962b6d\n| \"3\"\n| \n* sha1-624532f5d8a5839344ccfc465957a975e1962b6d\n| Parent: sha1-611d5d868352d4d6ae9b778d6627b81f769cdef5\n| \"2\"\n| \n* sha1-611d5d868352d4d6ae9b778d6627b81f769cdef5\n| Parent: None\n| \"1\"\n"
	diffRes1  = "* sha1-0b0a92f5515b5778194d07d8011b7c18bf9be178\n| Parent: sha1-14491727789ebd03ca0ddd5e3a1307ca3e2651dc\n| -   \"6\"\n| +   \"7\"\n| \n* sha1-14491727789ebd03ca0ddd5e3a1307ca3e2651dc\n| Parent: sha1-b5a8493d7be003673216bdc0ed275fbbacbb9b08\n| -   \"5\"\n| +   \"6\"\n| \n* sha1-b5a8493d7be003673216bdc0ed275fbbacbb9b08\n| Parent: sha1-2b837c2e2dd0e6ef015e4742bd969f9fa1641f38\n| -   \"4\"\n| +   \"5\"\n| \n*   sha1-2b837c2e2dd0e6ef015e4742bd969f9fa1641f38\n|\\  Merge: sha1-1f622d4006278a63cc77e95c6efcaf83f46606c4 sha1-eca65988a4ade585b8502d8f9fc37a5a5da94ab2\n| | -   \"3.6\"\n| | +   \"4\"\n| | \n| * sha1-eca65988a4ade585b8502d8f9fc37a5a5da94ab2\n| | Parent: sha1-890cf3b1d49228ace9d691a627131a2872e42eb4\n| | -   \"3.5\"\n| | +   \"3.7\"\n| | \n| *   sha1-890cf3b1d49228ace9d691a627131a2872e42eb4\n| |\\  Merge: sha1-4f72aeacef670bc6884a97de5a363b0674ccd008 sha1-fa78bedf601fc6d49ebf70865944dfccf5b5a133\n| | | -   \"3.1\"\n| | | +   \"3.5\"\n| | | \n| | * sha1-fa78bedf601fc6d49ebf70865944dfccf5b5a133\n| | | Parent: sha1-72f530e2ba8ba3168de8da8d15271cd856ac5c2b\n| | | -   \"3.1.5\"\n| | | +   \"3.1.7\"\n| | | \n| | * sha1-72f530e2ba8ba3168de8da8d15271cd856ac5c2b\n| | | Parent: sha1-dda04a898b2048558b4238a4f111364ff5c92ab8\n| | | -   \"3.1.3\"\n| | | +   \"3.1.5\"\n| | | \n* | | sha1-1f622d4006278a63cc77e95c6efcaf83f46606c4\n| | | Parent: sha1-e1583426c4201c7602b4aa1ae835915666d933f0\n| | | -   \"3.2\"\n| | | +   \"3.6\"\n| | | \n| | * sha1-dda04a898b2048558b4238a4f111364ff5c92ab8\n| | | Parent: sha1-4f72aeacef670bc6884a97de5a363b0674ccd008\n| | | -   \"3.1\"\n| | | +   \"3.1.3\"\n| | | \n* | | sha1-e1583426c4201c7602b4aa1ae835915666d933f0\n| |/  Parent: sha1-27290eda714ce5d47df05e8f77a0986647887e32\n| |   -   \"3\"\n| |   +   \"3.2\"\n| |   \n| * sha1-4f72aeacef670bc6884a97de5a363b0674ccd008\n|/  Parent: sha1-27290eda714ce5d47df05e8f77a0986647887e32\n|   -   \"3\"\n|   +   \"3.1\"\n|   \n* sha1-27290eda714ce5d47df05e8f77a0986647887e32\n| Parent: sha1-624532f5d8a5839344ccfc465957a975e1962b6d\n| -   \"2\"\n| +   \"3\"\n| \n* sha1-624532f5d8a5839344ccfc465957a975e1962b6d\n| Parent: sha1-611d5d868352d4d6ae9b778d6627b81f769cdef5\n| -   \"1\"\n| +   \"2\"\n| \n* sha1-611d5d868352d4d6ae9b778d6627b81f769cdef5\n| Parent: None\n| \n"

	graphRes2 = "*   sha1-65c7e849861df97129cbd49352e52eef6dad3b11\n|\\  Merge: sha1-4578a4c35f09b6fe85608a95145a36f4c701d030 sha1-aecf8991bf5af5bb0b725ff2bdeb260426508ac5\n| | \"101\"\n| | \n* |   sha1-4578a4c35f09b6fe85608a95145a36f4c701d030\n|\\ \\  Merge: sha1-611d5d868352d4d6ae9b778d6627b81f769cdef5 sha1-d9cf6dcbf03a014c28c80a9baa34525b4f9095c8\n| | | \"11\"\n| | | \n* | sha1-611d5d868352d4d6ae9b778d6627b81f769cdef5\n| | Parent: None\n| | \"1\"\n| | \n* sha1-d9cf6dcbf03a014c28c80a9baa34525b4f9095c8\n| Parent: None\n| \"10\"\n| \n* sha1-aecf8991bf5af5bb0b725ff2bdeb260426508ac5\n| Parent: None\n| \"100\"\n"
	diffRes2  = "*   sha1-65c7e849861df97129cbd49352e52eef6dad3b11\n|\\  Merge: sha1-4578a4c35f09b6fe85608a95145a36f4c701d030 sha1-aecf8991bf5af5bb0b725ff2bdeb260426508ac5\n| | -   \"11\"\n| | +   \"101\"\n| | \n* |   sha1-4578a4c35f09b6fe85608a95145a36f4c701d030\n|\\ \\  Merge: sha1-611d5d868352d4d6ae9b778d6627b81f769cdef5 sha1-d9cf6dcbf03a014c28c80a9baa34525b4f9095c8\n| | | -   \"1\"\n| | | +   \"11\"\n| | | \n* | sha1-611d5d868352d4d6ae9b778d6627b81f769cdef5\n| | Parent: None\n| | \n* sha1-d9cf6dcbf03a014c28c80a9baa34525b4f9095c8\n| Parent: None\n| \n* sha1-aecf8991bf5af5bb0b725ff2bdeb260426508ac5\n| Parent: None\n| \n"

	graphRes3 = "*   sha1-a859b0936cc42f073c03a27eb820f29a57f025f3\n|\\  Merge: sha1-d996bef5350a0316da794ae1f262dfcdcd4b2e8c sha1-65c99ad8e4db74b2cb3bf00bff806a4d46a220eb\n| | \"2222-wz\"\n| | \n* |   sha1-d996bef5350a0316da794ae1f262dfcdcd4b2e8c\n|\\ \\  Merge: sha1-da122844f28f7a814fb99a4950ff673ea7e3611c sha1-ba3e907b42eaba15bf01752b012c92f8e5821536\n| | | \"222-wy\"\n| | | \n| * |   sha1-ba3e907b42eaba15bf01752b012c92f8e5821536\n| |\\ \\  Merge: sha1-8af6c17c296c36f2b5adf19b02a83918a61088d1 sha1-624532f5d8a5839344ccfc465957a975e1962b6d\n| | | | \"22-wx\"\n| | | | \n* | | | sha1-da122844f28f7a814fb99a4950ff673ea7e3611c\n| | | | Parent: sha1-624532f5d8a5839344ccfc465957a975e1962b6d\n| | | | \"200-y\"\n| | | | \n| * | | sha1-8af6c17c296c36f2b5adf19b02a83918a61088d1\n| | | | Parent: sha1-624532f5d8a5839344ccfc465957a975e1962b6d\n| | | | \"20-x\"\n| | | | \n| | | * sha1-65c99ad8e4db74b2cb3bf00bff806a4d46a220eb\n|/ / /  Parent: sha1-624532f5d8a5839344ccfc465957a975e1962b6d\n|       \"2000-z\"\n|       \n* sha1-624532f5d8a5839344ccfc465957a975e1962b6d\n| Parent: sha1-611d5d868352d4d6ae9b778d6627b81f769cdef5\n| \"2\"\n| \n* sha1-611d5d868352d4d6ae9b778d6627b81f769cdef5\n| Parent: None\n| \"1\"\n"
	diffRes3  = "*   sha1-a859b0936cc42f073c03a27eb820f29a57f025f3\n|\\  Merge: sha1-d996bef5350a0316da794ae1f262dfcdcd4b2e8c sha1-65c99ad8e4db74b2cb3bf00bff806a4d46a220eb\n| | -   \"222-wy\"\n| | +   \"2222-wz\"\n| | \n* |   sha1-d996bef5350a0316da794ae1f262dfcdcd4b2e8c\n|\\ \\  Merge: sha1-da122844f28f7a814fb99a4950ff673ea7e3611c sha1-ba3e907b42eaba15bf01752b012c92f8e5821536\n| | | -   \"200-y\"\n| | | +   \"222-wy\"\n| | | \n| * |   sha1-ba3e907b42eaba15bf01752b012c92f8e5821536\n| |\\ \\  Merge: sha1-8af6c17c296c36f2b5adf19b02a83918a61088d1 sha1-624532f5d8a5839344ccfc465957a975e1962b6d\n| | | | -   \"20-x\"\n| | | | +   \"22-wx\"\n| | | | \n* | | | sha1-da122844f28f7a814fb99a4950ff673ea7e3611c\n| | | | Parent: sha1-624532f5d8a5839344ccfc465957a975e1962b6d\n| | | | -   \"2\"\n| | | | +   \"200-y\"\n| | | | \n| * | | sha1-8af6c17c296c36f2b5adf19b02a83918a61088d1\n| | | | Parent: sha1-624532f5d8a5839344ccfc465957a975e1962b6d\n| | | | -   \"2\"\n| | | | +   \"20-x\"\n| | | | \n| | | * sha1-65c99ad8e4db74b2cb3bf00bff806a4d46a220eb\n|/ / /  Parent: sha1-624532f5d8a5839344ccfc465957a975e1962b6d\n|       -   \"2\"\n|       +   \"2000-z\"\n|       \n* sha1-624532f5d8a5839344ccfc465957a975e1962b6d\n| Parent: sha1-611d5d868352d4d6ae9b778d6627b81f769cdef5\n| -   \"1\"\n| +   \"2\"\n| \n* sha1-611d5d868352d4d6ae9b778d6627b81f769cdef5\n| Parent: None\n| \n"

	truncRes1  = "* sha1-39d1f600887364b2e4832fe80f6853b0966a9e6c\n| Parent: sha1-185ad8e966cba1a70b7f6cf19cd7bc5a7983c3d2\n| List<String>([  // 11 items\n|   \"one\",\n|   \"two\",\n|   \"three\",\n|   \"four\",\n|   \"five\",\n|   \"six\",\n|   \"seven\",\n| ...\n| \n* sha1-185ad8e966cba1a70b7f6cf19cd7bc5a7983c3d2\n| Parent: None\n| \"the first line\"\n"
	diffTrunc1 = "* sha1-39d1f600887364b2e4832fe80f6853b0966a9e6c\n| Parent: sha1-185ad8e966cba1a70b7f6cf19cd7bc5a7983c3d2\n| -   \"the first line\"\n| +   [  // 11 items\n| +     \"one\",\n| +     \"two\",\n| +     \"three\",\n| +     \"four\",\n| +     \"five\",\n| +     \"six\",\n| ...\n| \n* sha1-185ad8e966cba1a70b7f6cf19cd7bc5a7983c3d2\n| Parent: None\n| \n"

	truncRes2  = "* sha1-39d1f600887364b2e4832fe80f6853b0966a9e6c\n| Parent: sha1-185ad8e966cba1a70b7f6cf19cd7bc5a7983c3d2\n| List<String>([  // 11 items\n|   \"one\",\n|   \"two\",\n|   \"three\",\n|   \"four\",\n|   \"five\",\n|   \"six\",\n|   \"seven\",\n|   \"eight\",\n|   \"nine\",\n|   \"ten\",\n|   \"eleven\",\n| ])\n| \n* sha1-185ad8e966cba1a70b7f6cf19cd7bc5a7983c3d2\n| Parent: None\n| \"the first line\"\n"
	diffTrunc2 = "* sha1-39d1f600887364b2e4832fe80f6853b0966a9e6c\n| Parent: sha1-185ad8e966cba1a70b7f6cf19cd7bc5a7983c3d2\n| -   \"the first line\"\n| +   [  // 11 items\n| +     \"one\",\n| +     \"two\",\n| +     \"three\",\n| +     \"four\",\n| +     \"five\",\n| +     \"six\",\n| +     \"seven\",\n| +     \"eight\",\n| +     \"nine\",\n| +     \"ten\",\n| +     \"eleven\",\n| +   ]\n| \n* sha1-185ad8e966cba1a70b7f6cf19cd7bc5a7983c3d2\n| Parent: None\n| \n"

	truncRes3  = "* sha1-39d1f600887364b2e4832fe80f6853b0966a9e6c\n| Parent: sha1-185ad8e966cba1a70b7f6cf19cd7bc5a7983c3d2\n* sha1-185ad8e966cba1a70b7f6cf19cd7bc5a7983c3d2\n| Parent: None\n"
	diffTrunc3 = "* sha1-39d1f600887364b2e4832fe80f6853b0966a9e6c\n| Parent: sha1-185ad8e966cba1a70b7f6cf19cd7bc5a7983c3d2\n* sha1-185ad8e966cba1a70b7f6cf19cd7bc5a7983c3d2\n| Parent: None\n"
)
