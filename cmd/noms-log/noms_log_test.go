package main

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/clients/go/flags"
	"github.com/attic-labs/noms/clients/go/test_util"
	"github.com/attic-labs/noms/datas"
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
	fmt.Printf("commit ref: %s, type: %s\n", commit.Ref(), commit.Type().Name())
	ds.Store().Close()
	s.Contains(s.Run(main, []string{spec}), commit.Ref().String())
}

func (s *nomsShowTestSuite) TestNomsLog() {
	datasetName := "dsTest"
	spec := fmt.Sprintf("ldb:%s:%s", s.LdbDir, datasetName)
	sp, err := flags.ParseDatasetSpec(spec)
	s.NoError(err)

	ds, err := sp.Dataset()
	s.NoError(err)
	ds.Store().Close()
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
	return newDs.CommitWithParents(types.NewString(v), datas.NewSetOfRefOfCommit().Insert(parentDs.HeadRef()))
}

func mergeDatasets(ds1, ds2 dataset.Dataset, v string) (dataset.Dataset, error) {
	return ds1.CommitWithParents(types.NewString(v), datas.NewSetOfRefOfCommit().Insert(ds1.HeadRef(), ds2.HeadRef()))
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

	b1.Store().Close()
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

	ba, err = mergeDatasets(ba, bc, "101")
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

	w, err = mergeDatasets(w, z, "2222-wz")
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
	t, err = addCommitWithValue(t, toNomsList(l))
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
	graphRes1 = "* sha1-611689ac2ab299731ef41f2ec343232519505e40\n| Parent: sha1-98dfc571a0f2cdb67fd6ccaf1f8" +
		"8ad3c934b569f\n| \"7\"\n| \n* sha1-98dfc571a0f2cdb67fd6ccaf1f88ad3c934b569f\n| Parent: sha1-8f4bed6" +
		"ca39e56521bb2c2d51e04b3c621d0b164\n| \"6\"\n| \n* sha1-8f4bed6ca39e56521bb2c2d51e04b3c621d0b164\n| " +
		"Parent: sha1-1579de9719780729f805d832cedf30f3dc514ab6\n| \"5\"\n| \n*   sha1-1579de9719780729f805d8" +
		"32cedf30f3dc514ab6\n|\\  Merge: sha1-ee04cc1dda6b010ca3488e4996d14a54176b169d sha1-3b3e72797f54d4d6" +
		"ea4cefe080643e1717e5fc89\n| | \"4\"\n| | \n| * sha1-3b3e72797f54d4d6ea4cefe080643e1717e5fc89\n| | P" +
		"arent: sha1-f67f4a5eafead91ddb0d090c2b350845c2d7c443\n| | \"3.7\"\n| | \n| *   sha1-f67f4a5eafead91" +
		"ddb0d090c2b350845c2d7c443\n| |\\  Merge: sha1-d392854a7b3fea30b8f30cb1db70e27d03c23c6e sha1-4da4474" +
		"c6c032fe3d5a7f6d051ac61142cc0777d\n| | | \"3.5\"\n| | | \n| * | sha1-d392854a7b3fea30b8f30cb1db70e2" +
		"7d03c23c6e\n| | | Parent: sha1-dee9a653168b3b8ef274b2ec60c1b2524306591d\n| | | \"3.1.7\"\n| | | \n|" +
		" * | sha1-dee9a653168b3b8ef274b2ec60c1b2524306591d\n| | | Parent: sha1-242e659af0ef0e376b877b64ffac" +
		"1ba42f70df69\n| | | \"3.1.5\"\n| | | \n* | | sha1-ee04cc1dda6b010ca3488e4996d14a54176b169d\n| | | P" +
		"arent: sha1-9b247a8497322fd362f98fd4f990bb175ca03908\n| | | \"3.6\"\n| | | \n| * | sha1-242e659af0e" +
		"f0e376b877b64ffac1ba42f70df69\n| | | Parent: sha1-4da4474c6c032fe3d5a7f6d051ac61142cc0777d\n| | | " +
		"\"3.1.3\"\n| | | \n* | | sha1-9b247a8497322fd362f98fd4f990bb175ca03908\n| |/  Parent: sha1-9443faf0" +
		"2f7495e53f3f1e87b180e328424f2830\n| |   \"3.2\"\n| |   \n| * sha1-4da4474c6c032fe3d5a7f6d051ac61142" +
		"cc0777d\n|/  Parent: sha1-9443faf02f7495e53f3f1e87b180e328424f2830\n|   \"3.1\"\n|   \n* sha1-9443f" +
		"af02f7495e53f3f1e87b180e328424f2830\n| Parent: sha1-c2961e584d41e98a7c735e399eef6c618e0431b6\n|" +
		" \"3\"\n| \n* sha1-c2961e584d41e98a7c735e399eef6c618e0431b6\n| Parent: sha1-4a1a4e051327f02c1be502a" +
		"c7ce9e7bf04fbf729\n| \"2\"\n| \n* sha1-4a1a4e051327f02c1be502ac7ce9e7bf04fbf729\n| Parent: None\n|" +
		" \"1\"\n"

	graphRes2 = "*   sha1-a7f6c6b7f0db1f9d2448bf23c4aa70d983dfecb2\n|\\  Merge: sha1-10473a7892604ff88d9" +
		"149e3cbb9dd9dc123d194 sha1-d37384e9e9cf2f9a0abd5968151c246fdd8cf9dd\n| | \"101\"\n| | \n| *   sha1-" +
		"d37384e9e9cf2f9a0abd5968151c246fdd8cf9dd\n| |\\  Merge: sha1-07cec20929f80a1fd923991683f4bf3adad099" +
		"03 sha1-4a1a4e051327f02c1be502ac7ce9e7bf04fbf729\n| | | \"11\"\n| | | \n* | sha1-10473a7892604ff88d" +
		"9149e3cbb9dd9dc123d194\n| | Parent: None\n| | \"100\"\n| | \n* sha1-07cec20929f80a1fd923991683f4bf3" +
		"adad09903\n| Parent: None\n| \"10\"\n| \n* sha1-4a1a4e051327f02c1be502ac7ce9e7bf04fbf729\n| Parent:" +
		" None\n| \"1\"\n"

	graphRes3 = "*   sha1-97eddb72fcb8b41be99d8f322bc4ddcd25cf9456\n|\\  Merge: sha1-1182ee3c6481e1c582c2f7ba" +
		"2d6f81754c44e263 sha1-c857de40c67a58e72e722de5bedd1c444ece8dd1\n| | \"2222-wz\"\n| | \n| *   sha1-c8" +
		"57de40c67a58e72e722de5bedd1c444ece8dd1\n| |\\  Merge: sha1-96a67ea3f5407c593dca7d71f98a8375dc1237dd " +
		"sha1-126475cc41d8ad7f38250d563a29d683eca21a80\n| | | \"222-wy\"\n| | | \n| * |   sha1-96a67ea3f5407c" +
		"593dca7d71f98a8375dc1237dd\n| |\\ \\  Merge: sha1-c2961e584d41e98a7c735e399eef6c618e0431b6 sha1-e824" +
		"5c30e79dbb2c882112b796b51d718f4e5984\n| | | | \"22-wx\"\n| | | | \n* | | | sha1-1182ee3c6481e1c582c2" +
		"f7ba2d6f81754c44e263\n| | | | Parent: sha1-c2961e584d41e98a7c735e399eef6c618e0431b6\n| | | | \"2000-" +
		"z\"\n| | | | \n| | * | sha1-e8245c30e79dbb2c882112b796b51d718f4e5984\n| | | | Parent: sha1-c2961e584" +
		"d41e98a7c735e399eef6c618e0431b6\n| | | | \"20-x\"\n| | | | \n| | | * sha1-126475cc41d8ad7f38250d563a" +
		"29d683eca21a80\n|/ / /  Parent: sha1-c2961e584d41e98a7c735e399eef6c618e0431b6\n|       \"200-y\"\n| " +
		"      \n* sha1-c2961e584d41e98a7c735e399eef6c618e0431b6\n| Parent: sha1-4a1a4e051327f02c1be502ac7ce9" +
		"e7bf04fbf729\n| \"2\"\n| \n* sha1-4a1a4e051327f02c1be502ac7ce9e7bf04fbf729\n| Parent: None\n| \"1\"\n"

	truncRes1 = "* sha1-a81bc1b23de202ef0e2275b5bef8449fc67fd863\n| Parent: sha1-81bc57a3956b56fa2bce22c6baf" +
		"81ebc5e9cac2c\n| List<Value>([\n|   \"one\",\n|   \"two\",\n|   \"three\",\n|   \"four\",\n|   \"fi" +
		"ve\",\n|   \"six\",\n|   \"seven\",\n| ...\n| \n* sha1-81bc57a3956b56fa2bce22c6baf81ebc5e9cac2c\n| " +
		"Parent: None\n| \"the first line\"\n"

	truncRes2 = "* sha1-a81bc1b23de202ef0e2275b5bef8449fc67fd863\n| Parent: sha1-81bc57a3956b56fa2bce22c6baf" +
		"81ebc5e9cac2c\n| List<Value>([\n|   \"one\",\n|   \"two\",\n|   \"three\",\n|   \"four\",\n|   \"fi" +
		"ve\",\n|   \"six\",\n|   \"seven\",\n|   \"eight\",\n|   \"nine\",\n|   \"ten\",\n|   \"eleven\",\n" +
		"| ])\n| \n* sha1-81bc57a3956b56fa2bce22c6baf81ebc5e9cac2c\n| Parent: None\n| \"the first line\"\n"

	truncRes3 = "* sha1-a81bc1b23de202ef0e2275b5bef8449fc67fd863\n| Parent: sha1-81bc57a3956b56fa2bce22c6baf" +
		"81ebc5e9cac2c\n| \n* sha1-81bc57a3956b56fa2bce22c6baf81ebc5e9cac2c\n| Parent: None\n"
)
