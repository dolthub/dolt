// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package url_fetch

import (
	"io/ioutil"
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/integrationtest"
)

const outDsName = "test-ds-out"

func TestIntegration(t *testing.T) {
	integrationtest.Run(t, &testSuite{})
}

type testSuite struct {
	integrationtest.IntegrationSuite
}

func (s *testSuite) Teardown() {
	db := s.Database()
	defer db.Close()

	outData := db.GetDataset(outDsName).HeadValue().(types.Blob)
	bs, err := ioutil.ReadAll(outData.Reader())
	s.NoError(err)
	expected := `<html><head></head><body><p>Hi. This is a Noms HTTP server.</p><p>To learn more, visit <a href="https://github.com/attic-labs/noms">our GitHub project</a>.</p></body></html>`
	s.Equal(expected, string(bs))
}

func (s *testSuite) NodeArgs() []string {
	// The db server root returns a small blurb of text that notifies the user
	// they are talking to a server. This is convenient since we can use that url
	// as our test case.
	serverUrl := s.DatabaseSpecString()
	outDsSpec := s.ValueSpecString(outDsName)
	return []string{serverUrl, outDsSpec}
}
