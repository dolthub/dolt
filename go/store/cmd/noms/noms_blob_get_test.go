// Copyright 2019 Dolthub, Inc.
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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/spec"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/clienttest"
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
	ctx := context.Background()
	db := sp.GetDatabase(ctx)
	vrw := sp.GetVRW(ctx)

	blobBytes := []byte("hello")
	blob, err := types.NewBlob(ctx, vrw, bytes.NewBuffer(blobBytes))
	s.NoError(err)

	ref, err := vrw.WriteValue(ctx, blob)
	s.NoError(err)

	ref, err = vrw.WriteValue(context.Background(), blob)
	s.NoError(err)
	ds, err := db.GetDataset(context.Background(), "datasetID")
	s.NoError(err)
	ds, err = db.GetDataset(context.Background(), "datasetID")
	s.NoError(err)
	_, err = datas.CommitValue(context.Background(), db, ds, ref)
	s.NoError(err)

	hashSpec := fmt.Sprintf("%s::#%s", s.TempDir, ref.TargetHash().String())
	filePath := filepath.Join(s.TempDir, "out")
	s.MustRun(main, []string{"blob", "export", hashSpec, filePath})

	fileBytes, err := os.ReadFile(filePath)
	s.NoError(err)
	s.Equal(blobBytes, fileBytes)

	stdout, _ := s.MustRun(main, []string{"blob", "export", hashSpec})
	fmt.Println("stdout:", stdout)
	s.Equal(blobBytes, []byte(stdout))
}
