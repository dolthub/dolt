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
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/spec"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/clienttest"
)

func TestNomsRoot(t *testing.T) {
	suite.Run(t, &nomsRootTestSuite{})
}

type nomsRootTestSuite struct {
	clienttest.ClientTestSuite
}

func (s *nomsRootTestSuite) TestBasic() {
	datasetName := "root-get"
	dsSpec := spec.CreateValueSpecString("nbs", s.DBDir, datasetName)
	sp, err := spec.ForDataset(dsSpec)
	s.NoError(err)
	defer sp.Close()

	ds := sp.GetDataset(context.Background())
	dbSpecStr := spec.CreateDatabaseSpecString("nbs", s.DBDir)
	db := ds.Database()

	goldenHello := "u8g2r4qg97kkqn42lvao77st2mv3bpl0\n"
	goldenGoodbye := "70b9adi6amrab3a5t4hcibdob0cq49m0\n"
	if types.Format_Default == types.Format_DOLT_DEV {
		goldenHello = "j1i5v3qbfnad2ucrorg1tg37ijfv7i4e\n"
		goldenGoodbye = "2d371t1mg01d66lbfl4er78kjscc7e5o\n"
	}

	ds, _ = datas.CommitValue(context.Background(), db, ds, types.String("hello!"))
	c1, _ := s.MustRun(main, []string{"root", dbSpecStr})
	s.Equal(goldenHello, c1)

	ds, _ = datas.CommitValue(context.Background(), db, ds, types.String("goodbye"))
	c2, _ := s.MustRun(main, []string{"root", dbSpecStr})
	s.Equal(goldenGoodbye, c2)

	// TODO: Would be good to test successful --update too, but requires changes to MustRun to allow
	// input because of prompt :(.
}
