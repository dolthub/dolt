// Copyright 2026 Dolthub, Inc.
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

package commands

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitParamFlags(t *testing.T) {
	prev := initParamFlags
	t.Cleanup(func() { initParamFlags = prev })
	initParamFlags = []initParamFlag{{name: "toy", desc: "A toy flag.", params: map[string]string{"toy_init": "1"}}}
	_, err := InitCmd{}.ArgParser().Parse([]string{"--toy"})
	require.NoError(t, err)

	ctx := context.Background()
	dEnv := createUninitializedEnv()
	defer dEnv.Close()
	result := InitCmd{}.Exec(ctx, "dolt init", []string{"-name", "Bill Billerson", "-email", "bigbillieb@fake.horse", "--toy"}, dEnv, nil)
	require.Equal(t, 0, result)
	assert.Equal(t, "1", dEnv.DBLoadParams["toy_init"], "the database loads with the flag's params")

	dEnv = createUninitializedEnv()
	defer dEnv.Close()
	result = InitCmd{}.Exec(ctx, "dolt init", []string{"-name", "Bill Billerson", "-email", "bigbillieb@fake.horse"}, dEnv, nil)
	require.Equal(t, 0, result)
	assert.NotContains(t, dEnv.DBLoadParams, "toy_init")
}
