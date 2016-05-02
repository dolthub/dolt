package main

import (
	"path"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/suite"
)

func TestShove(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	util.ClientTestSuite
}

func (s *testSuite) TestShove() {
	s.LdbFlagName = "-source-ldb"
	dbName := "databaseName"
	source1 := dataset.NewDataset(datas.NewDatabase(chunks.NewLevelDBStore(s.LdbDir, dbName, 1, false)), "foo")
	source1, err := source1.Commit(types.Number(42))
	s.NoError(err)
	source2, err := source1.Commit(types.Number(43))
	s.NoError(err)
	source1HeadRef := source1.Head().Ref()
	source2.DB().Close() // Close Database backing both Datasets

	ldb2dir := path.Join(s.TempDir, "ldb2")
	out := s.Run(main, []string{"-source-database", dbName, "-source", source1HeadRef.String(), "-sink-ldb", ldb2dir, "-sink-database", dbName, "-sink-ds", "bar"})
	s.Equal("", out)

	dest := dataset.NewDataset(datas.NewDatabase(chunks.NewLevelDBStore(ldb2dir, dbName, 1, false)), "bar")
	s.True(types.Number(42).Equals(dest.Head().Get(datas.ValueField)))
	dest.DB().Close()

	out = s.Run(main, []string{"-source-database", dbName, "-source", "foo", "-sink-ldb", ldb2dir, "-sink-ds", "bar"})
	s.Equal("", out)

	dest = dataset.NewDataset(datas.NewDatabase(chunks.NewLevelDBStore(ldb2dir, dbName, 1, false)), "bar")
	s.True(types.Number(43).Equals(dest.Head().Get(datas.ValueField)))
	dest.DB().Close()
}
