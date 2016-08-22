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
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/perf/suite"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
	humanize "github.com/dustin/go-humanize"
)

// CSV perf suites require the testdata directory to be checked out at $GOPATH/src/github.com/attic-labs/testdata (i.e. ../testdata relative to the noms directory).

// TODO: Add ny-vehicle-registrations test when CSV importing is faster (testdata/ny-vehicle-registrations/20150218.*).

type perfSuite struct {
	suite.PerfSuite
	csvImportExe string
	sfcBlobHash  hash.Hash
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

	var raw []io.Reader

	s.Pause(func() {
		// The raw data is split into a bunch of files foo.a, foo.b, etc.
		glob, err := filepath.Glob(path.Join(s.Testdata, "sf-crime", "2016-07-28.*"))
		assert.NoError(err)

		raw = make([]io.Reader, len(glob))
		for i, m := range glob {
			r, err := os.Open(m)
			assert.NoError(err)
			raw[i] = r
		}
	})

	defer s.Pause(func() {
		for _, r := range raw {
			assert.NoError(r.(io.ReadCloser).Close())
		}
	})

	blob := types.NewBlob(io.MultiReader(raw...))
	fmt.Fprintf(s.W, "csv/raw is %s\n", humanize.Bytes(blob.Len()))

	ds := dataset.NewDataset(s.Database, "csv/raw")
	_, err := ds.CommitValue(blob)
	assert.NoError(err)
}

func (s *perfSuite) Test02ImportSfCrimeCSVFromBlob() {
	assert := s.NewAssert()

	blobSpec := fmt.Sprintf("%s::csv/raw.value", s.DatabaseSpec)
	destSpec := fmt.Sprintf("%s::csv", s.DatabaseSpec)
	importCmd := exec.Command(s.csvImportExe, "-p", blobSpec, destSpec)
	importCmd.Stdout = s.W
	importCmd.Stderr = os.Stderr

	assert.NoError(importCmd.Run())
}

func TestPerf(t *testing.T) {
	suite.Run("csv-import", t, &perfSuite{})
}
