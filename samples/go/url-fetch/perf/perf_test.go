// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package perf

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/attic-labs/noms/go/perf/suite"
)

type perfSuite struct {
	suite.PerfSuite
	sfCrimePath string
	urlFetchExe string
}

func (s *perfSuite) SetupSuite() {
	assert := s.NewAssert()

	// The sf-crime data needs to be assembled for this test.
	sfCrime := s.TempFile()
	s.sfCrimePath = sfCrime.Name()

	sfCrimeFiles := s.OpenGlob(s.Testdata, "sf-crime", "2016-07-28.*")
	defer s.CloseGlob(sfCrimeFiles)

	_, err := io.Copy(sfCrime, io.MultiReader(sfCrimeFiles...))
	assert.NoError(err)
	sfCrime.Close()

	// Trick the temp file logic into creating a unique path for the url-fetch binary.
	exeFile := s.TempFile()
	s.urlFetchExe = exeFile.Name()

	exeFile.Close()
	os.Remove(s.urlFetchExe)

	err = exec.Command("go", "build", "-o", s.urlFetchExe, "github.com/attic-labs/noms/samples/go/url-fetch").Run()
	assert.NoError(err)
}

func (s *perfSuite) TestFetchFile() {
	s.importPath("file", s.sfCrimePath)
}

func (s *perfSuite) TestFetchURL() {
	assert := s.NewAssert()

	lstnr, err := net.Listen("tcp", ":0")
	assert.NoError(err)

	hndlr := http.FileServer(http.Dir(filepath.Dir(s.sfCrimePath)))
	servr := http.Server{Handler: hndlr}
	go func() {
		assert.NoError(servr.Serve(lstnr))
	}()

	s.importPath("url", fmt.Sprintf("http://%s/%s", lstnr.Addr().String(), filepath.Base(s.sfCrimePath)))
}

func (s *perfSuite) importPath(tag, path string) {
	host, stopFn := s.StartRemoteDatabase()
	defer stopFn()

	assert := s.NewAssert()
	cmd := exec.Command(s.urlFetchExe, path, fmt.Sprintf("%s::url-fetch-%s", host, tag))
	cmd.Stdout = s.W
	cmd.Stderr = os.Stderr
	assert.NoError(cmd.Run())
}

func TestPerf(t *testing.T) {
	suite.Run("url-fetch", t, &perfSuite{})
}
