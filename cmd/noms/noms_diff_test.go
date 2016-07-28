// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"

	"strings"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

type nomsDiffTestSuite struct {
	clienttest.ClientTestSuite
}

func TestNomsDiff(t *testing.T) {
	suite.Run(t, &nomsDiffTestSuite{})
}

func (s *nomsDiffTestSuite) TestNomsDiffOutputNotTruncated() {
	datasetName := "diffTest"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)
	ds, err := spec.GetDataset(str)
	s.NoError(err)

	ds, err = addCommit(ds, "first commit")
	s.NoError(err)
	r1 := spec.CreateValueSpecString("ldb", s.LdbDir, "#"+ds.HeadRef().TargetHash().String())

	ds, err = addCommit(ds, "second commit")
	s.NoError(err)
	r2 := spec.CreateValueSpecString("ldb", s.LdbDir, "#"+ds.HeadRef().TargetHash().String())

	ds.Database().Close()
	out, _ := s.Run(main, []string{"diff", r1, r2})
	s.True(strings.HasSuffix(out, "\"second commit\"\n  }\n"), out)
}
