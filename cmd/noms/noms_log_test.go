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
	sp, err := spec.ForDataset(spec.CreateValueSpecString("nbs", s.DBDir, "dsTest"))
	s.NoError(err)
	defer sp.Close()

	sp.GetDatabase() // create the database
	s.Panics(func() { s.MustRun(main, []string{"log", sp.String()}) })

	testCommitInResults(s, sp.String(), 1)
	testCommitInResults(s, sp.String(), 2)
}

func (s *nomsLogTestSuite) TestNomsLogPath() {
	sp, err := spec.ForPath(spec.CreateValueSpecString("nbs", s.DBDir, "dsTest.value.bar"))
	s.NoError(err)
	defer sp.Close()

	db := sp.GetDatabase()
	ds := sp.GetDataset()
	for i := 0; i < 3; i++ {
		data := types.NewStruct("", types.StructData{
			"bar": types.Number(i),
		})
		ds, err = db.CommitValue(ds, data)
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

	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("nbs", s.DBDir))
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

	dsSpec := spec.CreateValueSpecString("nbs", s.DBDir, "ds1")
	res, _ := s.MustRun(main, []string{"log", "--show-value=false", dsSpec})
	test.EqualsIgnoreHashes(s.T(), metaRes1, res)

	res, _ = s.MustRun(main, []string{"log", "--show-value=false", "--oneline", dsSpec})
	test.EqualsIgnoreHashes(s.T(), metaRes2, res)
}

func (s *nomsLogTestSuite) TestNomsGraph1() {
	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("nbs", s.DBDir))
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

	res, _ := s.MustRun(main, []string{"log", "--graph", "--show-value=true", spec.CreateValueSpecString("nbs", s.DBDir, "b1")})
	s.Equal(graphRes1, res)
	res, _ = s.MustRun(main, []string{"log", "--graph", "--show-value=false", spec.CreateValueSpecString("nbs", s.DBDir, "b1")})
	s.Equal(diffRes1, res)
}

func (s *nomsLogTestSuite) TestNomsGraph2() {
	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("nbs", s.DBDir))
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

	res, _ := s.MustRun(main, []string{"log", "--graph", "--show-value=true", spec.CreateValueSpecString("nbs", s.DBDir, "ba")})
	s.Equal(graphRes2, res)
	res, _ = s.MustRun(main, []string{"log", "--graph", "--show-value=false", spec.CreateValueSpecString("nbs", s.DBDir, "ba")})
	s.Equal(diffRes2, res)
}

func (s *nomsLogTestSuite) TestNomsGraph3() {
	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("nbs", s.DBDir))
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

	res, _ := s.MustRun(main, []string{"log", "--graph", "--show-value=true", spec.CreateValueSpecString("nbs", s.DBDir, "w")})
	test.EqualsIgnoreHashes(s.T(), graphRes3, res)
	res, _ = s.MustRun(main, []string{"log", "--graph", "--show-value=false", spec.CreateValueSpecString("nbs", s.DBDir, "w")})
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

	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("nbs", s.DBDir))
	s.NoError(err)
	defer sp.Close()

	t := sp.GetDatabase().GetDataset("truncate")

	t, err = addCommit(t, "the first line")
	s.NoError(err)

	l := []string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven"}
	_, err = addCommitWithValue(t, toNomsList(l))
	s.NoError(err)

	dsSpec := spec.CreateValueSpecString("nbs", s.DBDir, "truncate")
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
	graphRes1 = "* cmvkhq582litl19dtf9rdr27lkmmjl5a\n| Parent: n34kv1pmaq511ej6hpvqf6nun0nfsva6\n| \"7\"\n| \n* n34kv1pmaq511ej6hpvqf6nun0nfsva6\n| Parent: j9nk6bv9r7ep1j459j0mv2clof6s7792\n| \"6\"\n| \n* j9nk6bv9r7ep1j459j0mv2clof6s7792\n| Parent: 8rkr9of92el4fvg2quhflro7615roouc\n| \"5\"\n| \n*   8rkr9of92el4fvg2quhflro7615roouc\n|\\  Merge: r4c42m0u4k8g08ivo005p1k3e9c8a3tq n05ujdtqd9enisrbcrifhc6n41anur3g\n| | \"4\"\n| | \n| * n05ujdtqd9enisrbcrifhc6n41anur3g\n| | Parent: 6eu8a3l1gunugn7uinotiv8saf783pok\n| | \"3.7\"\n| | \n| *   6eu8a3l1gunugn7uinotiv8saf783pok\n| |\\  Merge: 92u4ja5p42gr6galrq7o5ubad98qk3pp f464nqgept56er12l7ikaj9jhrdrckb7\n| | | \"3.5\"\n| | | \n| | * f464nqgept56er12l7ikaj9jhrdrckb7\n| | | Parent: gjggba0bhcjd10kmooemvbvr4gnokdcm\n| | | \"3.1.7\"\n| | | \n| | * gjggba0bhcjd10kmooemvbvr4gnokdcm\n| | | Parent: q7gc5legocg4gq4qfd2v0i41sm9q2p9d\n| | | \"3.1.5\"\n| | | \n* | | r4c42m0u4k8g08ivo005p1k3e9c8a3tq\n| | | Parent: 885bl3ggjtnf9e4h4d3cnidh576hm27u\n| | | \"3.6\"\n| | | \n| | * q7gc5legocg4gq4qfd2v0i41sm9q2p9d\n| | | Parent: 92u4ja5p42gr6galrq7o5ubad98qk3pp\n| | | \"3.1.3\"\n| | | \n* | | 885bl3ggjtnf9e4h4d3cnidh576hm27u\n| |/  Parent: 7f8hmd1okp98ovnph695kumm4lknuqcd\n| |   \"3.2\"\n| |   \n| * 92u4ja5p42gr6galrq7o5ubad98qk3pp\n|/  Parent: 7f8hmd1okp98ovnph695kumm4lknuqcd\n|   \"3.1\"\n|   \n* 7f8hmd1okp98ovnph695kumm4lknuqcd\n| Parent: 2r6g3brn1867i66rri1suk49fev2js7b\n| \"3\"\n| \n* 2r6g3brn1867i66rri1suk49fev2js7b\n| Parent: ppu3smo89vu0bdukubgooo8efeo1i7q3\n| \"2\"\n| \n* ppu3smo89vu0bdukubgooo8efeo1i7q3\n| Parent: None\n| \"1\"\n"
	diffRes1  = "* cmvkhq582litl19dtf9rdr27lkmmjl5a\n| Parent: n34kv1pmaq511ej6hpvqf6nun0nfsva6\n| -   \"6\"\n| +   \"7\"\n| \n* n34kv1pmaq511ej6hpvqf6nun0nfsva6\n| Parent: j9nk6bv9r7ep1j459j0mv2clof6s7792\n| -   \"5\"\n| +   \"6\"\n| \n* j9nk6bv9r7ep1j459j0mv2clof6s7792\n| Parent: 8rkr9of92el4fvg2quhflro7615roouc\n| -   \"4\"\n| +   \"5\"\n| \n*   8rkr9of92el4fvg2quhflro7615roouc\n|\\  Merge: r4c42m0u4k8g08ivo005p1k3e9c8a3tq n05ujdtqd9enisrbcrifhc6n41anur3g\n| | -   \"3.6\"\n| | +   \"4\"\n| | \n| * n05ujdtqd9enisrbcrifhc6n41anur3g\n| | Parent: 6eu8a3l1gunugn7uinotiv8saf783pok\n| | -   \"3.5\"\n| | +   \"3.7\"\n| | \n| *   6eu8a3l1gunugn7uinotiv8saf783pok\n| |\\  Merge: 92u4ja5p42gr6galrq7o5ubad98qk3pp f464nqgept56er12l7ikaj9jhrdrckb7\n| | | -   \"3.1\"\n| | | +   \"3.5\"\n| | | \n| | * f464nqgept56er12l7ikaj9jhrdrckb7\n| | | Parent: gjggba0bhcjd10kmooemvbvr4gnokdcm\n| | | -   \"3.1.5\"\n| | | +   \"3.1.7\"\n| | | \n| | * gjggba0bhcjd10kmooemvbvr4gnokdcm\n| | | Parent: q7gc5legocg4gq4qfd2v0i41sm9q2p9d\n| | | -   \"3.1.3\"\n| | | +   \"3.1.5\"\n| | | \n* | | r4c42m0u4k8g08ivo005p1k3e9c8a3tq\n| | | Parent: 885bl3ggjtnf9e4h4d3cnidh576hm27u\n| | | -   \"3.2\"\n| | | +   \"3.6\"\n| | | \n| | * q7gc5legocg4gq4qfd2v0i41sm9q2p9d\n| | | Parent: 92u4ja5p42gr6galrq7o5ubad98qk3pp\n| | | -   \"3.1\"\n| | | +   \"3.1.3\"\n| | | \n* | | 885bl3ggjtnf9e4h4d3cnidh576hm27u\n| |/  Parent: 7f8hmd1okp98ovnph695kumm4lknuqcd\n| |   -   \"3\"\n| |   +   \"3.2\"\n| |   \n| * 92u4ja5p42gr6galrq7o5ubad98qk3pp\n|/  Parent: 7f8hmd1okp98ovnph695kumm4lknuqcd\n|   -   \"3\"\n|   +   \"3.1\"\n|   \n* 7f8hmd1okp98ovnph695kumm4lknuqcd\n| Parent: 2r6g3brn1867i66rri1suk49fev2js7b\n| -   \"2\"\n| +   \"3\"\n| \n* 2r6g3brn1867i66rri1suk49fev2js7b\n| Parent: ppu3smo89vu0bdukubgooo8efeo1i7q3\n| -   \"1\"\n| +   \"2\"\n| \n* ppu3smo89vu0bdukubgooo8efeo1i7q3\n| Parent: None\n| \n"

	graphRes2 = "*   3vtsgav7kotnm4t0g441oah0fhc8f612\n|\\  Merge: mlfoejp9rhuq4mlmedvceq46ecofu4fm 5tfaqaak42q0iq8p8d5ej8ju1p1o18t6\n| | \"101\"\n| | \n| *   5tfaqaak42q0iq8p8d5ej8ju1p1o18t6\n| |\\  Merge: ppu3smo89vu0bdukubgooo8efeo1i7q3 7f0obrqkr8pge2o0r6tgkub7jikl0638\n| | | \"11\"\n| | | \n* | mlfoejp9rhuq4mlmedvceq46ecofu4fm\n| | Parent: None\n| | \"100\"\n| | \n* ppu3smo89vu0bdukubgooo8efeo1i7q3\n| Parent: None\n| \"1\"\n| \n* 7f0obrqkr8pge2o0r6tgkub7jikl0638\n| Parent: None\n| \"10\"\n"
	diffRes2  = "*   3vtsgav7kotnm4t0g441oah0fhc8f612\n|\\  Merge: mlfoejp9rhuq4mlmedvceq46ecofu4fm 5tfaqaak42q0iq8p8d5ej8ju1p1o18t6\n| | -   \"100\"\n| | +   \"101\"\n| | \n| *   5tfaqaak42q0iq8p8d5ej8ju1p1o18t6\n| |\\  Merge: ppu3smo89vu0bdukubgooo8efeo1i7q3 7f0obrqkr8pge2o0r6tgkub7jikl0638\n| | | -   \"1\"\n| | | +   \"11\"\n| | | \n* | mlfoejp9rhuq4mlmedvceq46ecofu4fm\n| | Parent: None\n| | \n* ppu3smo89vu0bdukubgooo8efeo1i7q3\n| Parent: None\n| \n* 7f0obrqkr8pge2o0r6tgkub7jikl0638\n| Parent: None\n| \n"

	graphRes3 = "*   l2pilhhluk535j4620taktd87tr7bma3\n|\\  Merge: v4qkffjhedt7nmu1n8b95csd9g646iki mjneuuiveli2coui1qjm7rbr4acpaq7c\n| | \"2222-wz\"\n| | \n| *   mjneuuiveli2coui1qjm7rbr4acpaq7c\n| |\\  Merge: p5pgg27dcgclv02mlde0qfmuud21mmlj mpagh8od3kpjigrl6pt3atj5bofs8tel\n| | | \"222-wy\"\n| | | \n| | *   mpagh8od3kpjigrl6pt3atj5bofs8tel\n| | |\\  Merge: 2r6g3brn1867i66rri1suk49fev2js7b 5ve449uov4tl9f8gmgvf3jpj9cc32iu4\n| | | | \"22-wx\"\n| | | | \n* | | | v4qkffjhedt7nmu1n8b95csd9g646iki\n| | | | Parent: 2r6g3brn1867i66rri1suk49fev2js7b\n| | | | \"2000-z\"\n| | | | \n| * | | p5pgg27dcgclv02mlde0qfmuud21mmlj\n| | | | Parent: 2r6g3brn1867i66rri1suk49fev2js7b\n| | | | \"200-y\"\n| | | | \n| | | * 5ve449uov4tl9f8gmgvf3jpj9cc32iu4\n|/ / /  Parent: 2r6g3brn1867i66rri1suk49fev2js7b\n|       \"20-x\"\n|       \n* 2r6g3brn1867i66rri1suk49fev2js7b\n| Parent: ppu3smo89vu0bdukubgooo8efeo1i7q3\n| \"2\"\n| \n* ppu3smo89vu0bdukubgooo8efeo1i7q3\n| Parent: None\n| \"1\"\n"
	diffRes3  = "*   l2pilhhluk535j4620taktd87tr7bma3\n|\\  Merge: v4qkffjhedt7nmu1n8b95csd9g646iki mjneuuiveli2coui1qjm7rbr4acpaq7c\n| | -   \"2000-z\"\n| | +   \"2222-wz\"\n| | \n| *   mjneuuiveli2coui1qjm7rbr4acpaq7c\n| |\\  Merge: p5pgg27dcgclv02mlde0qfmuud21mmlj mpagh8od3kpjigrl6pt3atj5bofs8tel\n| | | -   \"200-y\"\n| | | +   \"222-wy\"\n| | | \n| | *   mpagh8od3kpjigrl6pt3atj5bofs8tel\n| | |\\  Merge: 2r6g3brn1867i66rri1suk49fev2js7b 5ve449uov4tl9f8gmgvf3jpj9cc32iu4\n| | | | -   \"2\"\n| | | | +   \"22-wx\"\n| | | | \n* | | | v4qkffjhedt7nmu1n8b95csd9g646iki\n| | | | Parent: 2r6g3brn1867i66rri1suk49fev2js7b\n| | | | -   \"2\"\n| | | | +   \"2000-z\"\n| | | | \n| * | | p5pgg27dcgclv02mlde0qfmuud21mmlj\n| | | | Parent: 2r6g3brn1867i66rri1suk49fev2js7b\n| | | | -   \"2\"\n| | | | +   \"200-y\"\n| | | | \n| | | * 5ve449uov4tl9f8gmgvf3jpj9cc32iu4\n|/ / /  Parent: 2r6g3brn1867i66rri1suk49fev2js7b\n|       -   \"2\"\n|       +   \"20-x\"\n|       \n* 2r6g3brn1867i66rri1suk49fev2js7b\n| Parent: ppu3smo89vu0bdukubgooo8efeo1i7q3\n| -   \"1\"\n| +   \"2\"\n| \n* ppu3smo89vu0bdukubgooo8efeo1i7q3\n| Parent: None\n| \n"

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
