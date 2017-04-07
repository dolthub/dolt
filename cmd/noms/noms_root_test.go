// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
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

	ds := sp.GetDataset()
	dbSpecStr := spec.CreateDatabaseSpecString("nbs", s.DBDir)
	ds, _ = ds.Database().CommitValue(ds, types.String("hello!"))
	c1, _ := s.MustRun(main, []string{"root", dbSpecStr})
	s.Equal("h9tlerpp1uqh1vgi06db8mp0qimtd6ob\n", c1)

	ds, _ = ds.Database().CommitValue(ds, types.String("goodbye"))
	c2, _ := s.MustRun(main, []string{"root", dbSpecStr})
	s.Equal("shfp8fdht6reca2bjg6iq5vtuk8phhqp\n", c2)

	// TODO: Would be good to test successful --update too, but requires changes to MustRun to allow
	// input because of prompt :(.
}
