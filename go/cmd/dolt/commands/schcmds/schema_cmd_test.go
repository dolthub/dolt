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

package schcmds

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

// Smoke test: dolt schema export runs successfully
func TestSchemaExport(t *testing.T) {
	dEnv, err := sqle.CreateEnvWithSeedData()
	require.NoError(t, err)

	args := []string{}
	commandStr := "dolt schema export"

	result := ExportCmd{}.Exec(context.TODO(), commandStr, args, dEnv)
	assert.Equal(t, 0, result)
}
