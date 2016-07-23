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
	blob_bytes := []byte("hello")
	blob := types.NewBlob(bytes.NewBuffer(blob_bytes))

	db, err := spec.GetDatabase(s.TempDir)
	s.NoError(err)
	hash := db.WriteValue(blob)
	db.Close()

	hash_spec := fmt.Sprintf("%s::#%s", s.TempDir, hash.TargetHash().String())
	file_path := filepath.Join(s.TempDir, "out")
	s.Run(main, []string{hash_spec, file_path})

	file_bytes, err := ioutil.ReadFile(file_path)
	s.NoError(err)
	s.Equal(blob_bytes, file_bytes)
}
