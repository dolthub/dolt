// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/spec"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/liquidata-inc/ld/dolt/go/store/util/clienttest"
	"github.com/stretchr/testify/suite"
)

func TestNomsBlobGet(t *testing.T) {
	suite.Run(t, &nbeSuite{})
}

type nbeSuite struct {
	clienttest.ClientTestSuite
}

func (s *nbeSuite) TestNomsBlobGet() {
	sp, err := spec.ForDatabase(s.TempDir)
	s.NoError(err)
	defer sp.Close()
	db := sp.GetDatabase(context.Background())

	blobBytes := []byte("hello")
	blob := types.NewBlob(context.Background(), db, bytes.NewBuffer(blobBytes))

	ref := db.WriteValue(context.Background(), blob)
	ds, err := db.GetDataset(context.Background(), "datasetID")
	s.NoError(err)
	_, err = db.CommitValue(context.Background(), ds, ref)
	s.NoError(err)

	hashSpec := fmt.Sprintf("%s::#%s", s.TempDir, ref.TargetHash().String())
	filePath := filepath.Join(s.TempDir, "out")
	s.MustRun(main, []string{"blob", "export", hashSpec, filePath})

	fileBytes, err := ioutil.ReadFile(filePath)
	s.NoError(err)
	s.Equal(blobBytes, fileBytes)

	stdout, _ := s.MustRun(main, []string{"blob", "export", hashSpec})
	fmt.Println("stdout:", stdout)
	s.Equal(blobBytes, []byte(stdout))
}
