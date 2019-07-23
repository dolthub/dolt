// Copyright 2019 Liquidata, Inc.
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
	"fmt"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/constants"
	"github.com/liquidata-inc/ld/dolt/go/store/util/clienttest"
	"github.com/stretchr/testify/suite"
)

func TestVersion(t *testing.T) {
	suite.Run(t, &nomsVersionTestSuite{})
}

type nomsVersionTestSuite struct {
	clienttest.ClientTestSuite
}

func (s *nomsVersionTestSuite) TestVersion() {
	val, _ := s.MustRun(main, []string{"version"})
	expectedVal := fmt.Sprintf("format version: %v\nbuilt from %v\n", constants.NomsVersion, constants.NomsGitSHA)
	s.Equal(val, expectedVal)
}
