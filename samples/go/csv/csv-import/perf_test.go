// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"testing"

	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/perf/suite"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/samples/go/csv"
	"github.com/attic-labs/testify/assert"
	humanize "github.com/dustin/go-humanize"
)

// CSV perf suites require the testdata directory to be checked out at $GOPATH/src/github.com/attic-labs/testdata (i.e. ../testdata relative to the noms directory).

type perfSuite struct {
	suite.PerfSuite
	csvImportExe string
}

func (s *perfSuite) SetupSuite() {
	// Trick the temp file logic into creating a unique path for the csv-import binary.
	f := s.TempFile("csv-import.perf_test")
	f.Close()
	os.Remove(f.Name())

	s.csvImportExe = f.Name()
	err := exec.Command("go", "build", "-o", s.csvImportExe, "github.com/attic-labs/noms/samples/go/csv/csv-import").Run()
	assert.NoError(s.T, err)
}

func (s *perfSuite) Test01ImportSfCrimeBlobFromTestdata() {
	assert := s.NewAssert()

	files := s.openGlob(s.Testdata, "sf-crime", "2016-07-28.*")
	defer s.closeGlob(files)

	blob := types.NewBlob(io.MultiReader(files...))
	fmt.Fprintf(s.W, "\tsf-crime is %s\n", humanize.Bytes(blob.Len()))

	ds := dataset.NewDataset(s.Database, "sf-crime/raw")
	_, err := ds.CommitValue(blob)
	assert.NoError(err)
}

func (s *perfSuite) Test02ImportSfCrimeCSVFromBlob() {
	s.execCsvImportExe("sf-crime")
}

func (s *perfSuite) Test03ImportSfRegisteredBusinessesFromBlobAsMap() {
	assert := s.NewAssert()

	files := s.openGlob(s.Testdata, "sf-registered-businesses", "2016-07-25.csv")
	defer s.closeGlob(files)

	blob := types.NewBlob(io.MultiReader(files...))
	fmt.Fprintf(s.W, "\tsf-reg-bus is %s\n", humanize.Bytes(blob.Len()))

	ds := dataset.NewDataset(s.Database, "sf-reg-bus/raw")
	_, err := ds.CommitValue(blob)
	assert.NoError(err)

	s.execCsvImportExe("sf-reg-bus", "--dest-type", "map:0")
}

func (s *perfSuite) Test04ImportChiBuildingViolationsBlobFromTestData() {
	assert := s.NewAssert()

	files := s.openGlob(s.Testdata, "chi-building-violations", "2016-07-30.csv.*")
	defer s.closeGlob(files)

	blob := types.NewBlob(io.MultiReader(files...))
	fmt.Fprintf(s.W, "\tchi-building-violations is %s\n", humanize.Bytes(blob.Len()))

	ds := dataset.NewDataset(s.Database, "chi-building-violations/raw")
	_, err := ds.CommitValue(blob)
	assert.NoError(err)
}

func (s *perfSuite) Test05ImportChiBuildingViolationsFromBlobAsMultiKeyMap() {
	s.execCsvImportExe("chi-building-violations", "--dest-type", "map:VIOLATION CODE,VIOLATION DATE")
}

func (s *perfSuite) execCsvImportExe(dsName string, args ...string) {
	assert := s.NewAssert()

	blobSpec := fmt.Sprintf("%s::%s/raw.value", s.DatabaseSpec, dsName)
	destSpec := fmt.Sprintf("%s::%s", s.DatabaseSpec, dsName)
	args = append(args, "-p", blobSpec, destSpec)
	importCmd := exec.Command(s.csvImportExe, args...)
	importCmd.Stdout = s.W
	importCmd.Stderr = os.Stderr

	assert.NoError(importCmd.Run())
}

func (s *perfSuite) TestParseSfCrime() {
	assert := s.NewAssert()

	files := s.openGlob(path.Join(s.Testdata, "sf-crime", "2016-07-28.*"))
	defer s.closeGlob(files)

	reader := csv.NewCSVReader(io.MultiReader(files...), ',')
	for {
		_, err := reader.Read()
		if err != nil {
			assert.Equal(io.EOF, err)
			break
		}
	}
}

// openGlob opens the concatenation of all files that match `pattern`, returned
// as []io.Reader so it can be used immediately with io.MultiReader.
//
// Large CSV files in testdata are broken up into foo.a, foo.b, etc to get
// around GitHub file size restrictions.
func (s *perfSuite) openGlob(pattern ...string) []io.Reader {
	assert := s.NewAssert()

	glob, err := filepath.Glob(path.Join(pattern...))
	assert.NoError(err)

	files := make([]io.Reader, len(glob))
	for i, m := range glob {
		f, err := os.Open(m)
		assert.NoError(err)
		files[i] = f
	}

	return files
}

// closeGlob closes all of the files, designed to be used with openGlob.
func (s *perfSuite) closeGlob(files []io.Reader) {
	assert := s.NewAssert()

	for _, f := range files {
		assert.NoError(f.(*os.File).Close())
	}
}

func TestPerf(t *testing.T) {
	suite.Run("csv-import", t, &perfSuite{})
}
