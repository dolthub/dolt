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

package envtestutils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	homeDir = "/users/home"
)

// CreateTestEnv creates an unitialized DoltEnv.  This is the state of a dolt repository before 'dolt init' is run
func CreateTestEnv(ctx context.Context) *env.DoltEnv {
	hdp := func() (s string, e error) {
		return homeDir, nil
	}

	fs := filesys.NewInMemFS([]string{homeDir + "/datasets/test"}, map[string][]byte{}, homeDir+"/datasets/test")
	return env.Load(ctx, hdp, fs, "mem://")
}

// CreateInitializedTestEnv creates an initialized DoltEnv.  This is the state of a dolt repository after 'dolt init' is
// run
func CreateInitializedTestEnv(t *testing.T, ctx context.Context) *env.DoltEnv {
	dEnv := CreateTestEnv(ctx)

	err := dEnv.InitRepo(ctx, types.Format_Default, "Ash Ketchum", "ash@poke.mon")
	require.NoError(t, err)

	return dEnv
}
