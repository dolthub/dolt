package main

import (
	"fmt"
	"path"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/clients/go/test_util"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/suite"
)

func TestSync(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	test_util.ClientTestSuite
}

func (s *testSuite) TestSync() {
	source1 := dataset.NewDataset(datas.NewDatabase(chunks.NewLevelDBStore(s.LdbDir, "", 1, false)), "foo")
	source1, err := source1.Commit(types.Number(42))
	s.NoError(err)
	source2, err := source1.Commit(types.Number(43))
	s.NoError(err)
	source1HeadRef := source1.Head().Ref()
	source2.Store().Close() // Close Database backing both Datasets

	sourceSpec := fmt.Sprintf("ldb:%s:%s", s.LdbDir, source1HeadRef)
	ldb2dir := path.Join(s.TempDir, "ldb2")
	sinkDatasetSpec := fmt.Sprintf("ldb:%s:%s", ldb2dir, "bar")
	out := s.Run(main, []string{sourceSpec, sinkDatasetSpec})
	s.Equal("", out)

	dest := dataset.NewDataset(datas.NewDatabase(chunks.NewLevelDBStore(ldb2dir, "", 1, false)), "bar")
	s.True(types.Number(42).Equals(dest.Head().Get(datas.ValueField)))
	dest.Store().Close()

	sourceDataset := fmt.Sprintf("ldb:%s:%s", s.LdbDir, "foo")
	out = s.Run(main, []string{sourceDataset, sinkDatasetSpec})
	s.Equal("", out)

	dest = dataset.NewDataset(datas.NewDatabase(chunks.NewLevelDBStore(ldb2dir, "", 1, false)), "bar")
	s.True(types.Number(43).Equals(dest.Head().Get(datas.ValueField)))
	dest.Store().Close()
}
