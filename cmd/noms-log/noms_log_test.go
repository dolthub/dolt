package main

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/clients/go/flags"
	"github.com/attic-labs/noms/clients/go/test_util"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestNomsShow(t *testing.T) {
	suite.Run(t, &nomsShowTestSuite{})
}

type nomsShowTestSuite struct {
	test_util.ClientTestSuite
}

func testCommitInResults(s *nomsShowTestSuite, spec string, i int) {
	sp, err := flags.ParseDatasetSpec(spec)
	s.NoError(err)
	ds, err := sp.Dataset()
	s.NoError(err)
	ds, err = ds.Commit(types.Number(1))
	s.NoError(err)
	commit := ds.Head()
	fmt.Printf("commit hash: %s, type: %s\n", commit.Hash(), commit.Type().Name())
	ds.Database().Close()
	s.Contains(s.Run(main, []string{spec}), commit.Hash().String())
}

func (s *nomsShowTestSuite) TestNomsLog() {
	datasetName := "dsTest"
	spec := fmt.Sprintf("ldb:%s:%s", s.LdbDir, datasetName)
	sp, err := flags.ParseDatasetSpec(spec)
	s.NoError(err)

	ds, err := sp.Dataset()
	s.NoError(err)
	ds.Database().Close()
	s.Equal("", s.Run(main, []string{spec}))

	testCommitInResults(s, spec, 1)
	testCommitInResults(s, spec, 2)
}

func addCommit(ds dataset.Dataset, v string) (dataset.Dataset, error) {
	return ds.Commit(types.NewString(v))
}

func addCommitWithValue(ds dataset.Dataset, v types.Value) (dataset.Dataset, error) {
	return ds.Commit(v)
}

func addBranchedDataset(newDs, parentDs dataset.Dataset, v string) (dataset.Dataset, error) {
	return newDs.CommitWithParents(types.NewString(v), types.NewSet().Insert(parentDs.HeadRef()))
}

func mergeDatasets(ds1, ds2 dataset.Dataset, v string) (dataset.Dataset, error) {
	return ds1.CommitWithParents(types.NewString(v), types.NewSet(ds1.HeadRef(), ds2.HeadRef()))
}

func (s *nomsShowTestSuite) TestNomsGraph1() {
	spec := fmt.Sprintf("ldb:%s", s.LdbDir)
	dbSpec, err := flags.ParseDatabaseSpec(spec)
	s.NoError(err)
	db, err := dbSpec.Database()
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
	s.Equal(graphRes1, s.Run(main, []string{"-graph", spec + ":b1"}))
}

func (s *nomsShowTestSuite) TestNomsGraph2() {
	spec := fmt.Sprintf("ldb:%s", s.LdbDir)
	dbSpec, err := flags.ParseDatabaseSpec(spec)
	s.NoError(err)
	db, err := dbSpec.Database()
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
	s.Equal(graphRes2, s.Run(main, []string{"-graph", spec + ":ba"}))
}

func (s *nomsShowTestSuite) TestNomsGraph3() {
	spec := fmt.Sprintf("ldb:%s", s.LdbDir)
	dbSpec, err := flags.ParseDatabaseSpec(spec)
	s.NoError(err)
	db, err := dbSpec.Database()
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
	s.Equal(graphRes3, s.Run(main, []string{"-graph", spec + ":w"}))
}

func (s *nomsShowTestSuite) TestTruncation() {
	toNomsList := func(l []string) types.List {
		nv := []types.Value{}
		for _, v := range l {
			nv = append(nv, types.NewString(v))
		}
		return types.NewList(nv...)
	}

	spec := fmt.Sprintf("ldb:%s", s.LdbDir)
	dbSpec, err := flags.ParseDatabaseSpec(spec)
	s.NoError(err)
	db, err := dbSpec.Database()
	s.NoError(err)

	t := dataset.NewDataset(db, "truncate")

	t, err = addCommit(t, "the first line")
	s.NoError(err)

	l := []string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven"}
	_, err = addCommitWithValue(t, toNomsList(l))
	s.NoError(err)
	db.Close()

	s.Equal(truncRes1, s.Run(main, []string{spec + ":truncate"}))
	s.Equal(truncRes2, s.Run(main, []string{"-max-lines=-1", spec + ":truncate"}))
	s.Equal(truncRes3, s.Run(main, []string{"-max-lines=0", spec + ":truncate"}))
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
	graphRes1 = "* sha1-301cfbf24447ddf59657f4bd39ff7a13434a75e3\n| Parent: sha1-dceaef8d85dcf5b7691b05351fef06c1a44274a9\n| \"7\"\n| \n* sha1-dceaef8d85dcf5b7691b05351fef06c1a44274a9\n| Parent: sha1-26c893b54611bb8e73dcbe10fbbda162c05ecbe9\n| \"6\"\n| \n* sha1-26c893b54611bb8e73dcbe10fbbda162c05ecbe9\n| Parent: sha1-920314785c450666cec766a9e64d982253fa7676\n| \"5\"\n| \n*   sha1-920314785c450666cec766a9e64d982253fa7676\n|\\  Merge: sha1-96cdf733fda5975fd0164fe0ada6ac5cd2876934 sha1-3062f7ff50e0bf98342afeff3222a8a8dbc74d73\n| | \"4\"\n| | \n* | sha1-96cdf733fda5975fd0164fe0ada6ac5cd2876934\n| | Parent: sha1-92284b7199b8f403df472dc1fba96c1d5176c978\n| | \"3.7\"\n| | \n* |   sha1-92284b7199b8f403df472dc1fba96c1d5176c978\n|\\ \\  Merge: sha1-1ed12c115e67b0b0fa0e0da7909724e9668b77b9 sha1-5a4b29ec2470924be5de428c825e0210d9b10609\n| | | \"3.5\"\n| | | \n| * | sha1-5a4b29ec2470924be5de428c825e0210d9b10609\n| | | Parent: sha1-ea084320d402c6c3b4dc08f043d573eb08e98e9c\n| | | \"3.1.7\"\n| | | \n| * | sha1-ea084320d402c6c3b4dc08f043d573eb08e98e9c\n| | | Parent: sha1-4c20213b26bb97a3a1e49fb49177719c5cf3375a\n| | | \"3.1.5\"\n| | | \n| * | sha1-4c20213b26bb97a3a1e49fb49177719c5cf3375a\n| | | Parent: sha1-1ed12c115e67b0b0fa0e0da7909724e9668b77b9\n| | | \"3.1.3\"\n| | | \n| | * sha1-3062f7ff50e0bf98342afeff3222a8a8dbc74d73\n|/  | Parent: sha1-66e8c17d3f9fa3793a256316033f536c9fe4a19a\n|   | \"3.6\"\n|   | \n* | sha1-1ed12c115e67b0b0fa0e0da7909724e9668b77b9\n| | Parent: sha1-e68b5130f512c30879b3670640aff9a1c2f2b520\n| | \"3.1\"\n| | \n| * sha1-66e8c17d3f9fa3793a256316033f536c9fe4a19a\n|/  Parent: sha1-e68b5130f512c30879b3670640aff9a1c2f2b520\n|   \"3.2\"\n|   \n* sha1-e68b5130f512c30879b3670640aff9a1c2f2b520\n| Parent: sha1-b68eaba21eb2ef14d308a6411e418702983f0078\n| \"3\"\n| \n* sha1-b68eaba21eb2ef14d308a6411e418702983f0078\n| Parent: sha1-efd21957109c61767d8c5d08b61371606e2cc229\n| \"2\"\n| \n* sha1-efd21957109c61767d8c5d08b61371606e2cc229\n| Parent: None\n| \"1\"\n"

	graphRes2 = "*   sha1-04fd7504223fec9d34c45d3e69a7e480aa3cb0e0\n|\\  Merge: sha1-2f8d9e19cb008ca2b95c92d64cfde86c706aedc1 sha1-a1563dd09db2eee74eeb951e8c2b846c856623f8\n| | \"101\"\n| | \n| *   sha1-a1563dd09db2eee74eeb951e8c2b846c856623f8\n| |\\  Merge: sha1-67668ec954466f0caaf03315d80dc6806555f8ec sha1-efd21957109c61767d8c5d08b61371606e2cc229\n| | | \"11\"\n| | | \n* | sha1-2f8d9e19cb008ca2b95c92d64cfde86c706aedc1\n| | Parent: None\n| | \"100\"\n| | \n* sha1-67668ec954466f0caaf03315d80dc6806555f8ec\n| Parent: None\n| \"10\"\n| \n* sha1-efd21957109c61767d8c5d08b61371606e2cc229\n| Parent: None\n| \"1\"\n"

	graphRes3 = "*   sha1-372cdc5cc6d00f50a43e7951c70b8a3d2a9a1f27\n|\\  Merge: sha1-8866f5b64b15cc20a0a1a44765a572223eaa3830 sha1-2eba6234e50a2cf14abb33da2285747ccaf63e26\n| | \"2222-wz\"\n| | \n* |   sha1-8866f5b64b15cc20a0a1a44765a572223eaa3830\n|\\ \\  Merge: sha1-91482e3b88a20ba32440e82a0a27793ce370ce77 sha1-a5c137eecd612dacd82074faa446b8437011eea0\n| | | \"222-wy\"\n| | | \n| * |   sha1-a5c137eecd612dacd82074faa446b8437011eea0\n| |\\ \\  Merge: sha1-8f0502e8da52872e73e80dd38d8494966dc2155f sha1-b68eaba21eb2ef14d308a6411e418702983f0078\n| | | | \"22-wx\"\n| | | | \n* | | | sha1-91482e3b88a20ba32440e82a0a27793ce370ce77\n| | | | Parent: sha1-b68eaba21eb2ef14d308a6411e418702983f0078\n| | | | \"200-y\"\n| | | | \n| * | | sha1-8f0502e8da52872e73e80dd38d8494966dc2155f\n| | | | Parent: sha1-b68eaba21eb2ef14d308a6411e418702983f0078\n| | | | \"20-x\"\n| | | | \n| | | * sha1-2eba6234e50a2cf14abb33da2285747ccaf63e26\n|/ / /  Parent: sha1-b68eaba21eb2ef14d308a6411e418702983f0078\n|       \"2000-z\"\n|       \n* sha1-b68eaba21eb2ef14d308a6411e418702983f0078\n| Parent: sha1-efd21957109c61767d8c5d08b61371606e2cc229\n| \"2\"\n| \n* sha1-efd21957109c61767d8c5d08b61371606e2cc229\n| Parent: None\n| \"1\"\n"

	truncRes1 = "* sha1-1552e08d34bc82ecb889c44cf80edd02379d0104\n| Parent: sha1-b9883cf06956658088b93ffbdc5e589418f82a37\n| List<String>([\n|   \"one\",\n|   \"two\",\n|   \"three\",\n|   \"four\",\n|   \"five\",\n|   \"six\",\n|   \"seven\",\n| ...\n| \n* sha1-b9883cf06956658088b93ffbdc5e589418f82a37\n| Parent: None\n| \"the first line\"\n"

	truncRes2 = "* sha1-1552e08d34bc82ecb889c44cf80edd02379d0104\n| Parent: sha1-b9883cf06956658088b93ffbdc5e589418f82a37\n| List<String>([\n|   \"one\",\n|   \"two\",\n|   \"three\",\n|   \"four\",\n|   \"five\",\n|   \"six\",\n|   \"seven\",\n|   \"eight\",\n|   \"nine\",\n|   \"ten\",\n|   \"eleven\",\n| ])\n| \n* sha1-b9883cf06956658088b93ffbdc5e589418f82a37\n| Parent: None\n| \"the first line\"\n"

	truncRes3 = "* sha1-1552e08d34bc82ecb889c44cf80edd02379d0104\n| Parent: sha1-b9883cf06956658088b93ffbdc5e589418f82a37\n| \n* sha1-b9883cf06956658088b93ffbdc5e589418f82a37\n| Parent: None\n"
)
