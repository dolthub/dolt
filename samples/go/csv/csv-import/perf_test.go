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
	"testing"

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
	f := s.TempFile()
	f.Close()
	os.Remove(f.Name())

	s.csvImportExe = f.Name()
	err := exec.Command("go", "build", "-o", s.csvImportExe, "github.com/attic-labs/noms/samples/go/csv/csv-import").Run()
	assert.NoError(s.T, err)
}

func (s *perfSuite) Test01ImportSfCrimeBlobFromTestdata() {
	assert := s.NewAssert()

	files := s.OpenGlob(s.Testdata, "sf-crime", "2016-07-28.*")
	defer s.CloseGlob(files)

	blob := types.NewBlob(files...)
	fmt.Fprintf(s.W, "\tsf-crime is %s\n", humanize.Bytes(blob.Len()))

	ds := s.Database.GetDataset("sf-crime/raw")
	_, err := s.Database.CommitValue(ds, blob)
	assert.NoError(err)
}

func (s *perfSuite) Test02ImportSfCrimeCSVFromBlob() {
	s.execCsvImportExe("sf-crime")
}

func (s *perfSuite) Test03ImportSfRegisteredBusinessesFromBlobAsMap() {
	assert := s.NewAssert()

	files := s.OpenGlob(s.Testdata, "sf-registered-businesses", "2016-07-25.csv")
	defer s.CloseGlob(files)

	blob := types.NewBlob(files...)
	fmt.Fprintf(s.W, "\tsf-reg-bus is %s\n", humanize.Bytes(blob.Len()))

	ds := s.Database.GetDataset("sf-reg-bus/raw")
	_, err := s.Database.CommitValue(ds, blob)
	assert.NoError(err)

	s.execCsvImportExe("sf-reg-bus", "--dest-type", "map:0")
}

func (s *perfSuite) Test04ImportSfRegisteredBusinessesFromBlobAsMultiKeyMap() {
	s.execCsvImportExe("sf-reg-bus", "--dest-type", "map:Zip_Code,Business_Start_Date")
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

	files := s.OpenGlob(path.Join(s.Testdata, "sf-crime", "2016-07-28.*"))
	defer s.CloseGlob(files)

	reader := csv.NewCSVReader(io.MultiReader(files...), ',')
	for {
		_, err := reader.Read()
		if err != nil {
			assert.Equal(io.EOF, err)
			break
		}
	}
}

func TestPerf(t *testing.T) {
	suite.Run("csv-import", t, &perfSuite{})
}
