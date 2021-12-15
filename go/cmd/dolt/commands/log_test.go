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

package commands

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/store/types"
)

func TestLog(t *testing.T) {
	dEnv := createUninitializedEnv()
	err := dEnv.InitRepo(context.Background(), types.Format_7_18, "Bill Billerson", "bigbillieb@fake.horse", env.DefaultInitBranch)

	if err != nil {
		t.Error("Failed to init repo")
	}

	cs, _ := doltdb.NewCommitSpec(env.DefaultInitBranch)
	commit, _ := dEnv.DoltDB.Resolve(context.Background(), cs, nil)
	meta, _ := commit.GetCommitMeta()
	require.Equal(t, "Bill Billerson", meta.Name)
}
