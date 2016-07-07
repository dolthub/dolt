package counter

import (
	"testing"

	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/integrationtest"
)

const dsName = "test-counter"

func TestIntegration(t *testing.T) {
	integrationtest.Run(t, &testSuite{})
}

type testSuite struct {
	integrationtest.IntegrationSuite
}

func (s *testSuite) Setup() {
	db := s.Database()
	defer db.Close()
	ds := dataset.NewDataset(db, dsName)
	_, err := ds.Commit(types.Number(42))
	s.NoError(err)
}

func (s *testSuite) Teardown() {
	s.Equal("43\n", s.NodeOutput())

	db := s.Database()
	defer db.Close()
	ds := dataset.NewDataset(db, dsName)
	s.True(ds.HeadValue().Equals(types.Number(43)))
}

func (s *testSuite) NodeArgs() []string {
	spec := s.ValueSpecString(dsName)
	return []string{spec}
}
