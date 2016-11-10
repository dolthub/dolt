// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

func TestBlobGet(t *testing.T) {
	suite.Run(t, &bgSuite{})
}

type bgSuite struct {
	clienttest.ClientTestSuite
}

func (s *bgSuite) TestBlobGet() {
	blobBytes := []byte("hello")
	blob := types.NewBlob(bytes.NewBuffer(blobBytes))

	sp, err := spec.ForDatabase(s.TempDir)
	s.NoError(err)
	defer sp.Close()
	hash := sp.GetDatabase().WriteValue(blob)

	hashSpec := fmt.Sprintf("%s::#%s", s.TempDir, hash.TargetHash().String())
	filePath := filepath.Join(s.TempDir, "out")
	s.MustRun(main, []string{hashSpec, filePath})

	fileBytes, err := ioutil.ReadFile(filePath)
	s.NoError(err)
	s.Equal(blobBytes, fileBytes)
}
