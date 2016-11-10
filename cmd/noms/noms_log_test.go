// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"

	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/noms/go/util/test"
	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"
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

	sp.GetDatabase().CommitValue(sp.GetDataset(), types.Number(i))
	s.NoError(err)

	commit := sp.GetDataset().Head()
	res, _ := s.MustRun(main, []string{"log", str})
	s.Contains(res, commit.Hash().String())
}

func (s *nomsLogTestSuite) TestNomsLog() {
	sp, err := spec.ForDataset(spec.CreateValueSpecString("ldb", s.LdbDir, "dsTest"))
	s.NoError(err)
	defer sp.Close()

	sp.GetDatabase() // create the database
	s.Panics(func() { s.MustRun(main, []string{"log", sp.Spec}) })

	testCommitInResults(s, sp.Spec, 1)
	testCommitInResults(s, sp.Spec, 2)
}

func addCommit(ds datas.Dataset, v string) (datas.Dataset, error) {
	return ds.Database().CommitValue(ds, types.String(v))
}

func addCommitWithValue(ds datas.Dataset, v types.Value) (datas.Dataset, error) {
	return ds.Database().CommitValue(ds, v)
}

func addBranchedDataset(newDs, parentDs datas.Dataset, v string) (datas.Dataset, error) {
	p := types.NewSet(parentDs.HeadRef())
	return newDs.Database().Commit(newDs, types.String(v), datas.CommitOptions{Parents: p})
}

func mergeDatasets(ds1, ds2 datas.Dataset, v string) (datas.Dataset, error) {
	p := types.NewSet(ds1.HeadRef(), ds2.HeadRef())
	return ds1.Database().Commit(ds1, types.String(v), datas.CommitOptions{Parents: p})
}

func (s *nomsLogTestSuite) TestNArg() {
	dsName := "nArgTest"

	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("ldb", s.LdbDir))
	s.NoError(err)
	defer sp.Close()

	ds := sp.GetDatabase().GetDataset(dsName)

	ds, err = addCommit(ds, "1")
	h1 := ds.Head().Hash()
	s.NoError(err)
	ds, err = addCommit(ds, "2")
	s.NoError(err)
	h2 := ds.Head().Hash()
	ds, err = addCommit(ds, "3")
	s.NoError(err)
	h3 := ds.Head().Hash()

	dsSpec := spec.CreateValueSpecString("ldb", s.LdbDir, dsName)
	res, _ := s.MustRun(main, []string{"log", "-n1", dsSpec})
	s.NotContains(res, h1.String())
	res, _ = s.MustRun(main, []string{"log", "-n0", dsSpec})
	s.Contains(res, h3.String())
	s.Contains(res, h2.String())
	s.Contains(res, h1.String())

	vSpec := spec.CreateValueSpecString("ldb", s.LdbDir, "#"+h3.String())
	res, _ = s.MustRun(main, []string{"log", "-n1", vSpec})
	s.NotContains(res, h1.String())
	res, _ = s.MustRun(main, []string{"log", "-n0", vSpec})
	s.Contains(res, h3.String())
	s.Contains(res, h2.String())
	s.Contains(res, h1.String())
}

func (s *nomsLogTestSuite) TestEmptyCommit() {
	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("ldb", s.LdbDir))
	s.NoError(err)
	defer sp.Close()

	db := sp.GetDatabase()
	ds := db.GetDataset("ds1")

	meta := types.NewStruct("Meta", map[string]types.Value{
		"longNameForTest": types.String("Yoo"),
		"test2":           types.String("Hoo"),
	})
	ds, err = db.Commit(ds, types.String("1"), datas.CommitOptions{Meta: meta})
	s.NoError(err)

	ds, err = db.Commit(ds, types.String("2"), datas.CommitOptions{})
	s.NoError(err)

	dsSpec := spec.CreateValueSpecString("ldb", s.LdbDir, "ds1")
	res, _ := s.MustRun(main, []string{"log", "--show-value=false", dsSpec})
	test.EqualsIgnoreHashes(s.T(), metaRes1, res)

	res, _ = s.MustRun(main, []string{"log", "--show-value=false", "--oneline", dsSpec})
	test.EqualsIgnoreHashes(s.T(), metaRes2, res)
}

func (s *nomsLogTestSuite) TestNomsGraph1() {
	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("ldb", s.LdbDir))
	s.NoError(err)
	defer sp.Close()

	db := sp.GetDatabase()

	b1 := db.GetDataset("b1")
	b1, err = addCommit(b1, "1")
	s.NoError(err)
	b1, err = addCommit(b1, "2")
	s.NoError(err)
	b1, err = addCommit(b1, "3")
	s.NoError(err)

	b2 := db.GetDataset("b2")
	b2, err = addBranchedDataset(b2, b1, "3.1")
	s.NoError(err)

	b1, err = addCommit(b1, "3.2")
	s.NoError(err)
	b1, err = addCommit(b1, "3.6")
	s.NoError(err)

	b3 := db.GetDataset("b3")
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

	res, _ := s.MustRun(main, []string{"log", "--graph", "--show-value=true", spec.CreateValueSpecString("ldb", s.LdbDir, "b1")})
	s.Equal(graphRes1, res)
	res, _ = s.MustRun(main, []string{"log", "--graph", "--show-value=false", spec.CreateValueSpecString("ldb", s.LdbDir, "b1")})
	s.Equal(diffRes1, res)
}

func (s *nomsLogTestSuite) TestNomsGraph2() {
	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("ldb", s.LdbDir))
	s.NoError(err)
	defer sp.Close()

	db := sp.GetDatabase()

	ba := db.GetDataset("ba")
	ba, err = addCommit(ba, "1")
	s.NoError(err)

	bb := db.GetDataset("bb")
	bb, err = addCommit(bb, "10")
	s.NoError(err)

	bc := db.GetDataset("bc")
	bc, err = addCommit(bc, "100")
	s.NoError(err)

	ba, err = mergeDatasets(ba, bb, "11")
	s.NoError(err)

	_, err = mergeDatasets(ba, bc, "101")
	s.NoError(err)

	res, _ := s.MustRun(main, []string{"log", "--graph", "--show-value=true", spec.CreateValueSpecString("ldb", s.LdbDir, "ba")})
	s.Equal(graphRes2, res)
	res, _ = s.MustRun(main, []string{"log", "--graph", "--show-value=false", spec.CreateValueSpecString("ldb", s.LdbDir, "ba")})
	s.Equal(diffRes2, res)
}

func (s *nomsLogTestSuite) TestNomsGraph3() {
	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("ldb", s.LdbDir))
	s.NoError(err)
	defer sp.Close()

	db := sp.GetDatabase()

	w := db.GetDataset("w")

	w, err = addCommit(w, "1")
	s.NoError(err)

	w, err = addCommit(w, "2")
	s.NoError(err)

	x := db.GetDataset("x")
	x, err = addBranchedDataset(x, w, "20-x")
	s.NoError(err)

	y := db.GetDataset("y")
	y, err = addBranchedDataset(y, w, "200-y")
	s.NoError(err)

	z := db.GetDataset("z")
	z, err = addBranchedDataset(z, w, "2000-z")
	s.NoError(err)

	w, err = mergeDatasets(w, x, "22-wx")
	s.NoError(err)

	w, err = mergeDatasets(w, y, "222-wy")
	s.NoError(err)

	_, err = mergeDatasets(w, z, "2222-wz")
	s.NoError(err)

	res, _ := s.MustRun(main, []string{"log", "--graph", "--show-value=true", spec.CreateValueSpecString("ldb", s.LdbDir, "w")})
	test.EqualsIgnoreHashes(s.T(), graphRes3, res)
	res, _ = s.MustRun(main, []string{"log", "--graph", "--show-value=false", spec.CreateValueSpecString("ldb", s.LdbDir, "w")})
	test.EqualsIgnoreHashes(s.T(), diffRes3, res)
}

func (s *nomsLogTestSuite) TestTruncation() {
	toNomsList := func(l []string) types.List {
		nv := []types.Value{}
		for _, v := range l {
			nv = append(nv, types.String(v))
		}
		return types.NewList(nv...)
	}

	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("ldb", s.LdbDir))
	s.NoError(err)
	defer sp.Close()

	t := sp.GetDatabase().GetDataset("truncate")

	t, err = addCommit(t, "the first line")
	s.NoError(err)

	l := []string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven"}
	_, err = addCommitWithValue(t, toNomsList(l))
	s.NoError(err)

	dsSpec := spec.CreateValueSpecString("ldb", s.LdbDir, "truncate")
	res, _ := s.MustRun(main, []string{"log", "--graph", "--show-value=true", dsSpec})
	test.EqualsIgnoreHashes(s.T(), truncRes1, res)
	res, _ = s.MustRun(main, []string{"log", "--graph", "--show-value=false", dsSpec})
	test.EqualsIgnoreHashes(s.T(), diffTrunc1, res)

	res, _ = s.MustRun(main, []string{"log", "--graph", "--show-value=true", "--max-lines=-1", dsSpec})
	test.EqualsIgnoreHashes(s.T(), truncRes2, res)
	res, _ = s.MustRun(main, []string{"log", "--graph", "--show-value=false", "--max-lines=-1", dsSpec})
	test.EqualsIgnoreHashes(s.T(), diffTrunc2, res)

	res, _ = s.MustRun(main, []string{"log", "--graph", "--show-value=true", "--max-lines=0", dsSpec})
	test.EqualsIgnoreHashes(s.T(), truncRes3, res)
	res, _ = s.MustRun(main, []string{"log", "--graph", "--show-value=false", "--max-lines=0", dsSpec})
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
	graphRes1 = "* no94ratauqqhsdrbc3t78nsnso13u8r3\n| Parent: o8t1qrtuhcup7mr6eck29riclgjmnums\n| \"7\"\n| \n* o8t1qrtuhcup7mr6eck29riclgjmnums\n| Parent: 6kj15vb3bq8l4akmdun7sak37eeol6mq\n| \"6\"\n| \n* 6kj15vb3bq8l4akmdun7sak37eeol6mq\n| Parent: irbjvvvg1seqdmg411kk7mp50a5m765d\n| \"5\"\n| \n*   irbjvvvg1seqdmg411kk7mp50a5m765d\n|\\  Merge: cc549g1o5573qievvcdq71eecea0ru7f d3r3jqf3t80b0calp98uv70f9snhvrml\n| | \"4\"\n| | \n| * d3r3jqf3t80b0calp98uv70f9snhvrml\n| | Parent: ifeosk3m6nkb8jmgs1c61jp6pal5mr7r\n| | \"3.7\"\n| | \n| *   ifeosk3m6nkb8jmgs1c61jp6pal5mr7r\n| |\\  Merge: jo5egeh23414i1ltbprrh3rla7lb43cq 2kcqcip76aka7m7dcf6t5k1tgho9snpa\n| | | \"3.5\"\n| | | \n| * | jo5egeh23414i1ltbprrh3rla7lb43cq\n| | | Parent: 8d1sj5p6dvpi4e09cpcbt9a8m70q3vui\n| | | \"3.1.7\"\n| | | \n| * | 8d1sj5p6dvpi4e09cpcbt9a8m70q3vui\n| | | Parent: d5kadvihdea9omvsfvkei3nv3q85keu2\n| | | \"3.1.5\"\n| | | \n* | | cc549g1o5573qievvcdq71eecea0ru7f\n| | | Parent: om2jno6lpb23mvct2gfs54j18gtd0hb5\n| | | \"3.6\"\n| | | \n| * | d5kadvihdea9omvsfvkei3nv3q85keu2\n| | | Parent: 2kcqcip76aka7m7dcf6t5k1tgho9snpa\n| | | \"3.1.3\"\n| | | \n* | | om2jno6lpb23mvct2gfs54j18gtd0hb5\n| |/  Parent: lelp8sk1be0s03dasflpajhfvbfquktu\n| |   \"3.2\"\n| |   \n| * 2kcqcip76aka7m7dcf6t5k1tgho9snpa\n|/  Parent: lelp8sk1be0s03dasflpajhfvbfquktu\n|   \"3.1\"\n|   \n* lelp8sk1be0s03dasflpajhfvbfquktu\n| Parent: esd5lqno0falqp25upnmo4ihffbqerqj\n| \"3\"\n| \n* esd5lqno0falqp25upnmo4ihffbqerqj\n| Parent: mu3kl33om7qr4ieggqv0fuggv4lpphhf\n| \"2\"\n| \n* mu3kl33om7qr4ieggqv0fuggv4lpphhf\n| Parent: None\n| \"1\"\n"
	diffRes1  = "* no94ratauqqhsdrbc3t78nsnso13u8r3\n| Parent: o8t1qrtuhcup7mr6eck29riclgjmnums\n| -   \"6\"\n| +   \"7\"\n| \n* o8t1qrtuhcup7mr6eck29riclgjmnums\n| Parent: 6kj15vb3bq8l4akmdun7sak37eeol6mq\n| -   \"5\"\n| +   \"6\"\n| \n* 6kj15vb3bq8l4akmdun7sak37eeol6mq\n| Parent: irbjvvvg1seqdmg411kk7mp50a5m765d\n| -   \"4\"\n| +   \"5\"\n| \n*   irbjvvvg1seqdmg411kk7mp50a5m765d\n|\\  Merge: cc549g1o5573qievvcdq71eecea0ru7f d3r3jqf3t80b0calp98uv70f9snhvrml\n| | -   \"3.6\"\n| | +   \"4\"\n| | \n| * d3r3jqf3t80b0calp98uv70f9snhvrml\n| | Parent: ifeosk3m6nkb8jmgs1c61jp6pal5mr7r\n| | -   \"3.5\"\n| | +   \"3.7\"\n| | \n| *   ifeosk3m6nkb8jmgs1c61jp6pal5mr7r\n| |\\  Merge: jo5egeh23414i1ltbprrh3rla7lb43cq 2kcqcip76aka7m7dcf6t5k1tgho9snpa\n| | | -   \"3.1.7\"\n| | | +   \"3.5\"\n| | | \n| * | jo5egeh23414i1ltbprrh3rla7lb43cq\n| | | Parent: 8d1sj5p6dvpi4e09cpcbt9a8m70q3vui\n| | | -   \"3.1.5\"\n| | | +   \"3.1.7\"\n| | | \n| * | 8d1sj5p6dvpi4e09cpcbt9a8m70q3vui\n| | | Parent: d5kadvihdea9omvsfvkei3nv3q85keu2\n| | | -   \"3.1.3\"\n| | | +   \"3.1.5\"\n| | | \n* | | cc549g1o5573qievvcdq71eecea0ru7f\n| | | Parent: om2jno6lpb23mvct2gfs54j18gtd0hb5\n| | | -   \"3.2\"\n| | | +   \"3.6\"\n| | | \n| * | d5kadvihdea9omvsfvkei3nv3q85keu2\n| | | Parent: 2kcqcip76aka7m7dcf6t5k1tgho9snpa\n| | | -   \"3.1\"\n| | | +   \"3.1.3\"\n| | | \n* | | om2jno6lpb23mvct2gfs54j18gtd0hb5\n| |/  Parent: lelp8sk1be0s03dasflpajhfvbfquktu\n| |   -   \"3\"\n| |   +   \"3.2\"\n| |   \n| * 2kcqcip76aka7m7dcf6t5k1tgho9snpa\n|/  Parent: lelp8sk1be0s03dasflpajhfvbfquktu\n|   -   \"3\"\n|   +   \"3.1\"\n|   \n* lelp8sk1be0s03dasflpajhfvbfquktu\n| Parent: esd5lqno0falqp25upnmo4ihffbqerqj\n| -   \"2\"\n| +   \"3\"\n| \n* esd5lqno0falqp25upnmo4ihffbqerqj\n| Parent: mu3kl33om7qr4ieggqv0fuggv4lpphhf\n| -   \"1\"\n| +   \"2\"\n| \n* mu3kl33om7qr4ieggqv0fuggv4lpphhf\n| Parent: None\n| \n"

	graphRes2 = "*   f496n4vfmambio9ifqgcv7psvitq46h4\n|\\  Merge: 9l1k8c5uik2hhu1emtif331mieli49db 4ks4t12b6kakergphhaca6m8b0bsccga\n| | \"101\"\n| | \n* |   9l1k8c5uik2hhu1emtif331mieli49db\n|\\ \\  Merge: mu3kl33om7qr4ieggqv0fuggv4lpphhf tfe9rip1kugud6vvgl9qcvdd9jj3gio1\n| | | \"11\"\n| | | \n* | mu3kl33om7qr4ieggqv0fuggv4lpphhf\n| | Parent: None\n| | \"1\"\n| | \n* tfe9rip1kugud6vvgl9qcvdd9jj3gio1\n| Parent: None\n| \"10\"\n| \n* 4ks4t12b6kakergphhaca6m8b0bsccga\n| Parent: None\n| \"100\"\n"
	diffRes2  = "*   f496n4vfmambio9ifqgcv7psvitq46h4\n|\\  Merge: 9l1k8c5uik2hhu1emtif331mieli49db 4ks4t12b6kakergphhaca6m8b0bsccga\n| | -   \"11\"\n| | +   \"101\"\n| | \n* |   9l1k8c5uik2hhu1emtif331mieli49db\n|\\ \\  Merge: mu3kl33om7qr4ieggqv0fuggv4lpphhf tfe9rip1kugud6vvgl9qcvdd9jj3gio1\n| | | -   \"1\"\n| | | +   \"11\"\n| | | \n* | mu3kl33om7qr4ieggqv0fuggv4lpphhf\n| | Parent: None\n| | \n* tfe9rip1kugud6vvgl9qcvdd9jj3gio1\n| Parent: None\n| \n* 4ks4t12b6kakergphhaca6m8b0bsccga\n| Parent: None\n| \n"

	graphRes3 = "*   k81k6h8qfak0olqs3mklq9pbjh61srba\n|\\  Merge: fqg2d04nuk10kbbe9m8vfen7l4ashmnc gtbdds4hsqa02kqgacrsglouat3tavfv\n| | \"2222-wz\"\n| | \n| *   gtbdds4hsqa02kqgacrsglouat3tavfv\n| |\\  Merge: 55a0vlrjlvaqak6cqig6bg249h5gfl0t 0gvkuvvo502tsutnmlpbsp45bi3p135b\n| | | \"222-wy\"\n| | | \n| * |   55a0vlrjlvaqak6cqig6bg249h5gfl0t\n| |\\ \\  Merge: esd5lqno0falqp25upnmo4ihffbqerqj cliug26ajdb0js7caibn9ov1g8nch2jm\n| | | | \"22-wx\"\n| | | | \n* | | | fqg2d04nuk10kbbe9m8vfen7l4ashmnc\n| | | | Parent: esd5lqno0falqp25upnmo4ihffbqerqj\n| | | | \"2000-z\"\n| | | | \n| | * | cliug26ajdb0js7caibn9ov1g8nch2jm\n| | | | Parent: esd5lqno0falqp25upnmo4ihffbqerqj\n| | | | \"20-x\"\n| | | | \n| | | * 0gvkuvvo502tsutnmlpbsp45bi3p135b\n|/ / /  Parent: esd5lqno0falqp25upnmo4ihffbqerqj\n|       \"200-y\"\n|       \n* esd5lqno0falqp25upnmo4ihffbqerqj\n| Parent: mu3kl33om7qr4ieggqv0fuggv4lpphhf\n| \"2\"\n| \n* mu3kl33om7qr4ieggqv0fuggv4lpphhf\n| Parent: None\n| \"1\"\n"
	diffRes3  = "*   k81k6h8qfak0olqs3mklq9pbjh61srba\n|\\  Merge: fqg2d04nuk10kbbe9m8vfen7l4ashmnc gtbdds4hsqa02kqgacrsglouat3tavfv\n| | -   \"2000-z\"\n| | +   \"2222-wz\"\n| | \n| *   gtbdds4hsqa02kqgacrsglouat3tavfv\n| |\\  Merge: 55a0vlrjlvaqak6cqig6bg249h5gfl0t 0gvkuvvo502tsutnmlpbsp45bi3p135b\n| | | -   \"22-wx\"\n| | | +   \"222-wy\"\n| | | \n| * |   55a0vlrjlvaqak6cqig6bg249h5gfl0t\n| |\\ \\  Merge: esd5lqno0falqp25upnmo4ihffbqerqj cliug26ajdb0js7caibn9ov1g8nch2jm\n| | | | -   \"2\"\n| | | | +   \"22-wx\"\n| | | | \n* | | | fqg2d04nuk10kbbe9m8vfen7l4ashmnc\n| | | | Parent: esd5lqno0falqp25upnmo4ihffbqerqj\n| | | | -   \"2\"\n| | | | +   \"2000-z\"\n| | | | \n| | * | cliug26ajdb0js7caibn9ov1g8nch2jm\n| | | | Parent: esd5lqno0falqp25upnmo4ihffbqerqj\n| | | | -   \"2\"\n| | | | +   \"20-x\"\n| | | | \n| | | * 0gvkuvvo502tsutnmlpbsp45bi3p135b\n|/ / /  Parent: esd5lqno0falqp25upnmo4ihffbqerqj\n|       -   \"2\"\n|       +   \"200-y\"\n|       \n* esd5lqno0falqp25upnmo4ihffbqerqj\n| Parent: mu3kl33om7qr4ieggqv0fuggv4lpphhf\n| -   \"1\"\n| +   \"2\"\n| \n* mu3kl33om7qr4ieggqv0fuggv4lpphhf\n| Parent: None\n| \n"

	truncRes1  = "* p1442asfqnhgv1ebg6rijhl3kb9n4vt3\n| Parent: 4tq9si4tk8n0pead7hovehcbuued45sa\n| List<String>([  // 11 items\n|   \"one\",\n|   \"two\",\n|   \"three\",\n|   \"four\",\n|   \"five\",\n|   \"six\",\n|   \"seven\",\n| ...\n| \n* 4tq9si4tk8n0pead7hovehcbuued45sa\n| Parent: None\n| \"the first line\"\n"
	diffTrunc1 = "* p1442asfqnhgv1ebg6rijhl3kb9n4vt3\n| Parent: 4tq9si4tk8n0pead7hovehcbuued45sa\n| -   \"the first line\"\n| +   [  // 11 items\n| +     \"one\",\n| +     \"two\",\n| +     \"three\",\n| +     \"four\",\n| +     \"five\",\n| +     \"six\",\n| ...\n| \n* 4tq9si4tk8n0pead7hovehcbuued45sa\n| Parent: None\n| \n"

	truncRes2  = "* p1442asfqnhgv1ebg6rijhl3kb9n4vt3\n| Parent: 4tq9si4tk8n0pead7hovehcbuued45sa\n| List<String>([  // 11 items\n|   \"one\",\n|   \"two\",\n|   \"three\",\n|   \"four\",\n|   \"five\",\n|   \"six\",\n|   \"seven\",\n|   \"eight\",\n|   \"nine\",\n|   \"ten\",\n|   \"eleven\",\n| ])\n| \n* 4tq9si4tk8n0pead7hovehcbuued45sa\n| Parent: None\n| \"the first line\"\n"
	diffTrunc2 = "* p1442asfqnhgv1ebg6rijhl3kb9n4vt3\n| Parent: 4tq9si4tk8n0pead7hovehcbuued45sa\n| -   \"the first line\"\n| +   [  // 11 items\n| +     \"one\",\n| +     \"two\",\n| +     \"three\",\n| +     \"four\",\n| +     \"five\",\n| +     \"six\",\n| +     \"seven\",\n| +     \"eight\",\n| +     \"nine\",\n| +     \"ten\",\n| +     \"eleven\",\n| +   ]\n| \n* 4tq9si4tk8n0pead7hovehcbuued45sa\n| Parent: None\n| \n"

	truncRes3  = "* p1442asfqnhgv1ebg6rijhl3kb9n4vt3\n| Parent: 4tq9si4tk8n0pead7hovehcbuued45sa\n* 4tq9si4tk8n0pead7hovehcbuued45sa\n| Parent: None\n"
	diffTrunc3 = "* p1442asfqnhgv1ebg6rijhl3kb9n4vt3\n| Parent: 4tq9si4tk8n0pead7hovehcbuued45sa\n* 4tq9si4tk8n0pead7hovehcbuued45sa\n| Parent: None\n"

	metaRes1 = "p7jmuh67vhfccnqk1bilnlovnms1m67o\nParent: f8gjiv5974ojir9tnrl2k393o4s1tf0r\n-   \"1\"\n+   \"2\"\n\nf8gjiv5974ojir9tnrl2k393o4s1tf0r\nParent:          None\nLongNameForTest: \"Yoo\"\nTest2:           \"Hoo\"\n\n"
	metaRes2 = "p7jmuh67vhfccnqk1bilnlovnms1m67o (Parent: f8gjiv5974ojir9tnrl2k393o4s1tf0r)\nf8gjiv5974ojir9tnrl2k393o4s1tf0r (Parent: None)\n"
)
