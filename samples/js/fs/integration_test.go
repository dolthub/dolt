// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package fs

import (
	"io/ioutil"
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/integrationtest"
)

const dsName = "test-fs"

func TestIntegration(t *testing.T) {
	integrationtest.Run(t, &testSuite{})
}

type testSuite struct {
	integrationtest.IntegrationSuite
}

func (s *testSuite) NodeArgs() []string {
	return []string{"./test-data.txt", s.ValueSpecString(dsName)}
}

func (s *testSuite) Teardown() {
	out := s.NodeOutput()
	s.Contains(out, "1 of 1 entries")
	s.Contains(out, "done")

	db := s.Database()
	defer db.Close()
	ds := db.GetDataset(dsName)
	v := ds.HeadValue()
	s.True(v.Type().Equals(types.MakeStructType("File",
		[]string{"content"},
		[]*types.Type{
			types.MakeRefType(types.BlobType),
		},
	)))
	s.Equal("File", v.(types.Struct).Type().Desc.(types.StructDesc).Name)
	b := v.(types.Struct).Get("content").(types.Ref).TargetValue(db).(types.Blob)

	bs, err := ioutil.ReadAll(b.Reader())
	s.NoError(err)
	s.Equal([]byte("Hello World!\n"), bs)
}
