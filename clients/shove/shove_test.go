package main

import (
	"path"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
)

func TestShove(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	util.ClientTestSuite
}

func (s *testSuite) TestShove() {
	s.LdbFlagName = "-source-ldb"
	cs := chunks.NewLevelDBStore(s.LdbDir, 1, false)
	ds := dataset.NewDataset(datas.NewDataStore(cs), "foo")
	ds, ok := ds.Commit(types.Int32(42))
	s.True(ok)
	ds.Close()

	ldb2dir := path.Join(s.TempDir, "ldb2")
	out := s.Run(main, []string{"-source-ds", "foo", "-sink-ldb", ldb2dir, "-sink-ds", "bar"})
	s.Equal("", out)

	cs2 := chunks.NewLevelDBStore(ldb2dir, 1, false)
	ds2 := dataset.NewDataset(datas.NewDataStore(cs2), "bar")
	s.True(types.Int32(42).Equals(ds2.Head().Value()))
}
