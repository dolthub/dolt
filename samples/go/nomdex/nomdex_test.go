// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"regexp"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"
)

func TestCSVImporter(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type TestObj struct {
	Key    int
	Fname  string
	Lname  string
	Gender string
	Age    int
}

type testSuite struct {
	clienttest.ClientTestSuite
}

func TestNomdex(t *testing.T) {
	suite.Run(t, &testSuite{})
}

func makeTestDb(s *testSuite, dsId string) datas.Database {
	db := datas.NewDatabase(chunks.NewLevelDBStore(s.LdbDir, "", 1, false))
	l1 := []TestObj{
		{1, "will", "smith", "m", 40},
		{2, "lana", "turner", "f", 91},
		{3, "john", "wayne", "m", 86},
		{4, "johnny", "depp", "m", 50},
		{5, "merrill", "streep", "f", 60},
		{6, "rob", "courdry", "m", 45},
		{7, "bruce", "lee", "m", 72},
		{8, "bruce", "willis", "m", 36},
		{9, "luis", "bunuel", "m", 100},
		{10, "andy", "sandberg", "m", 32},
		{11, "walter", "coggins", "m", 28},
		{12, "seth", "rogan", "m", 29},
	}

	m1 := map[string]TestObj{
		"lg": {13, "lady", "gaga", "f", 39},
		"ss": {14, "sam", "smith", "m", 28},
		"rp": {15, "robert", "plant", "m", 69},
		"ml": {16, "meat", "loaf", "m", 65},
		"gf": {17, "glenn", "frey", "m", 60},
		"jr": {18, "joey", "ramone", "m", 55},
		"rc": {19, "ray", "charles", "m", 72},
		"bk": {20, "bb", "king", "m", 77},
		"b":  {21, "beck", "", "m", 38},
		"md": {22, "miles", "davis", "m", 82},
		"rd": {23, "roger", "daltry", "m", 62},
		"jf": {24, "john", "fogerty", "m", 60},
	}

	m := map[string]interface{}{"actors": l1, "musicians": m1}
	v, err := marshal.Marshal(m)
	s.NoError(err)
	_, err = db.CommitValue(db.GetDataset(dsId), v)
	s.NoError(err)
	return db
}

func (s *testSuite) TestNomdex() {
	dsId := "data"
	db := makeTestDb(s, dsId)
	s.NotNil(db)
	db.Close()

	fnameIdx := "fname-idx"
	dataSpec := spec.CreateValueSpecString("ldb", s.LdbDir, dsId)
	dbSpec := spec.CreateDatabaseSpecString("ldb", s.LdbDir)
	stdout, stderr := s.MustRun(main, []string{"up", "--out-ds", fnameIdx, "--in-path", dataSpec, "--by", ".fname"})
	s.Contains(stdout, "Indexed 24 objects")
	s.Equal("", stderr)

	genderIdx := "gender-idx"
	stdout, stderr = s.MustRun(main, []string{"up", "--out-ds", genderIdx, "--in-path", dataSpec, "--by", ".gender"})
	s.Contains(stdout, "Indexed 24 objects")
	s.Equal("", stderr)

	stdout, stderr = s.MustRun(main, []string{"find", "--db", dbSpec, `fname-idx = "lady"`})
	s.Contains(stdout, "Found 1 objects")
	s.Equal("", stderr)

	stdout, stderr = s.MustRun(main, []string{"find", "--db", dbSpec, `fname-idx = "lady" and gender-idx = "f"`})
	s.Contains(stdout, "Found 1 objects")
	s.Equal("", stderr)

	stdout, stderr = s.MustRun(main, []string{"find", "--db", dbSpec, `fname-idx != "lady" and gender-idx != "m"`})
	s.Contains(stdout, "Found 2 objects")
	s.Equal("", stderr)

	stdout, stderr = s.MustRun(main, []string{"find", "--db", dbSpec, `fname-idx != "lady" and fname-idx != "john"`})
	s.Contains(stdout, "Found 21 objects")
	s.Equal("", stderr)

	stdout, stderr = s.MustRun(main, []string{"find", "--db", dbSpec, `fname-idx != "lady" or gender-idx != "f"`})
	s.Contains(stdout, "Found 23 objects")
	s.Equal("", stderr)
}

func TestTransform(t *testing.T) {
	assert := assert.New(t)

	tcs := [][]string{
		[]string{`"01/02/2003"`, "\"(\\d{2})/(\\d{2})/(\\d{4})\"", "$3/$2/$1", "2003/02/01"},
	}

	for _, tc := range tcs {
		base, regex, replace, expected := tc[0], tc[1], tc[2], tc[3]

		testRe := regexp.MustCompile(regex)
		result := testRe.ReplaceAllString(base, replace)
		assert.Equal(expected, result)
	}

	tcs = [][]string{
		[]string{"343 STATE ST\nROCHESTER, NY 14650\n(43.161276, -77.619386)", "43.161276", "-77.619386"},
		[]string{"TWO EMBARCADERO CENTER\nPROMENADE LEVEL SAN FRANCISCO, CA 94111\n", "", ""},
	}

	findLatRe := regexp.MustCompile("(?s)\\(([\\d.]+)")
	findLngRe := regexp.MustCompile("(?s)(-?[\\d.]+)\\)")
	for _, tc := range tcs {
		base, expectedLat, expectedLng := tc[0], tc[1], tc[2]

		lat := findLatRe.FindStringSubmatch(base)
		assert.True(len(lat) == 0 && expectedLat == "" || (len(lat) == 2 && expectedLat == lat[1]))

		lng := findLngRe.FindStringSubmatch(base)
		assert.True(len(lng) == 0 && expectedLng == "" || (len(lng) == 2 && expectedLng == lng[1]))
	}
}
