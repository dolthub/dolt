// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"

	"strings"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
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
	sp, err := spec.ForDataset(spec.CreateValueSpecString("nbs", s.DBDir, "diffTest"))
	s.NoError(err)
	defer sp.Close()

	ds, err := addCommit(sp.GetDataset(), "first commit")
	s.NoError(err)
	r1 := spec.CreateValueSpecString("nbs", s.DBDir, "#"+ds.HeadRef().TargetHash().String())

	ds, err = addCommit(ds, "second commit")
	s.NoError(err)
	r2 := spec.CreateValueSpecString("nbs", s.DBDir, "#"+ds.HeadRef().TargetHash().String())

	out, _ := s.MustRun(main, []string{"diff", r1, r2})
	s.True(strings.HasSuffix(out, "\"second commit\"\n  }\n"), out)
}

func (s *nomsDiffTestSuite) TestNomsDiffSummarize() {
	sp, err := spec.ForDataset(spec.CreateValueSpecString("nbs", s.DBDir, "diffSummarizeTest"))
	s.NoError(err)
	defer sp.Close()

	db := sp.GetDatabase()

	ds, err := addCommit(sp.GetDataset(), "first commit")
	s.NoError(err)
	r1 := spec.CreateHashSpecString("nbs", s.DBDir, ds.HeadRef().TargetHash())

	ds, err = addCommit(ds, "second commit")
	s.NoError(err)
	r2 := spec.CreateHashSpecString("nbs", s.DBDir, ds.HeadRef().TargetHash())

	out, _ := s.MustRun(main, []string{"diff", "--summarize", r1, r2})
	s.Contains(out, "Comparing commit values")
	s.Contains(out, "1 insertion (100.00%), 1 deletion (100.00%), 0 changes (0.00%), (1 value vs 1 value)")

	out, _ = s.MustRun(main, []string{"diff", "--summarize", r1 + ".value", r2 + ".value"})
	s.NotContains(out, "Comparing commit values")

	ds, err = db.CommitValue(ds, types.NewList(types.Number(1), types.Number(2), types.Number(3), types.Number(4)))
	s.NoError(err)
	r3 := spec.CreateHashSpecString("nbs", s.DBDir, ds.HeadRef().TargetHash()) + ".value"

	ds, err = db.CommitValue(ds, types.NewList(types.Number(1), types.Number(222), types.Number(4)))
	s.NoError(err)
	r4 := spec.CreateHashSpecString("nbs", s.DBDir, ds.HeadRef().TargetHash()) + ".value"

	out, _ = s.MustRun(main, []string{"diff", "--summarize", r3, r4})
	s.Contains(out, "1 insertion (25.00%), 2 deletions (50.00%), 0 changes (0.00%), (4 values vs 3 values)")
}
