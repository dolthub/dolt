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
	sn := "storeName"
	source1 := dataset.NewDataset(datas.NewDataStore(chunks.NewLevelDBStore(s.LdbDir, sn, 1, false)), "foo")
	source1, err := source1.Commit(types.Int32(42))
	s.NoError(err)
	source2, err := source1.Commit(types.Int32(43))
	s.NoError(err)
	source1HeadRef := source1.Head().Ref()
	source2.Store().Close() // Close DataStore backing both Datasets

	ldb2dir := path.Join(s.TempDir, "ldb2")
	out := s.Run(main, []string{"-source-store", sn, "-source", source1HeadRef.String(), "-sink-ldb", ldb2dir, "-sink-store", sn, "-sink-ds", "bar"})
	s.Equal("", out)

	dest := dataset.NewDataset(datas.NewDataStore(chunks.NewLevelDBStore(ldb2dir, sn, 1, false)), "bar")
	s.True(types.Int32(42).Equals(dest.Head().Value()))
	dest.Store().Close()

	out = s.Run(main, []string{"-source-store", sn, "-source", "foo", "-sink-ldb", ldb2dir, "-sink-ds", "bar"})
	s.Equal("", out)

	dest = dataset.NewDataset(datas.NewDataStore(chunks.NewLevelDBStore(ldb2dir, sn, 1, false)), "bar")
	s.True(types.Int32(43).Equals(dest.Head().Value()))
	dest.Store().Close()
}
