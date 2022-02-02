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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/spec"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/clienttest"
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

	ds, err := addCommit(sp.GetDataset(context.Background()), "first commit")
	s.NoError(err)

	r1 := spec.CreateValueSpecString("nbs", s.DBDir, "#"+mustHeadRef(ds).TargetHash().String())

	ds, err = addCommit(ds, "second commit")
	s.NoError(err)
	r2 := spec.CreateValueSpecString("nbs", s.DBDir, "#"+mustHeadRef(ds).TargetHash().String())

	out, _ := s.MustRun(main, []string{"diff", r1, r2})
	s.True(strings.HasSuffix(out, "\"second commit\"\n  }\n"), out)
}

func (s *nomsDiffTestSuite) TestNomsDiffStat() {
	sp, err := spec.ForDataset(spec.CreateValueSpecString("nbs", s.DBDir, "diffStatTest"))
	s.NoError(err)
	defer sp.Close()

	db := sp.GetDatabase(context.Background())
	vrw := sp.GetVRW(context.Background())

	ds, err := addCommit(sp.GetDataset(context.Background()), "first commit")
	s.NoError(err)

	r1 := spec.CreateHashSpecString("nbs", s.DBDir, mustHeadRef(ds).TargetHash())

	ds, err = addCommit(ds, "second commit")
	s.NoError(err)
	r2 := spec.CreateHashSpecString("nbs", s.DBDir, mustHeadRef(ds).TargetHash())

	out, _ := s.MustRun(main, []string{"diff", "--stat", r1, r2})
	s.Contains(out, "Comparing commit values")
	s.Contains(out, "1 insertion (100.00%), 1 deletion (100.00%), 0 changes (0.00%), (1 value vs 1 value)")

	out, _ = s.MustRun(main, []string{"diff", "--stat", r1 + ".value", r2 + ".value"})
	s.NotContains(out, "Comparing commit values")

	l, err := types.NewList(context.Background(), vrw, types.Float(1), types.Float(2), types.Float(3), types.Float(4))
	s.NoError(err)
	ds, err = datas.CommitValue(context.Background(), db, ds, l)
	s.NoError(err)

	r3 := spec.CreateHashSpecString("nbs", s.DBDir, mustHeadRef(ds).TargetHash()) + ".value"

	l, err = types.NewList(context.Background(), vrw, types.Float(1), types.Float(222), types.Float(4))
	s.NoError(err)
	ds, err = datas.CommitValue(context.Background(), db, ds, l)
	s.NoError(err)
	r4 := spec.CreateHashSpecString("nbs", s.DBDir, mustHeadRef(ds).TargetHash()) + ".value"

	out, _ = s.MustRun(main, []string{"diff", "--stat", r3, r4})
	s.Contains(out, "1 insertion (25.00%), 2 deletions (50.00%), 0 changes (0.00%), (4 values vs 3 values)")
}
