// Copyright 2023 Dolthub, Inc.
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

package cluster

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
)

func TestCommitHookStartsNotCaughtUp(t *testing.T) {
	srcEnv := dtestutils.CreateTestEnv()
	t.Cleanup(func() {
		srcEnv.DoltDB.Close()
	})
	destEnv := dtestutils.CreateTestEnv()
	t.Cleanup(func() {
		destEnv.DoltDB.Close()
	})

	hook := newCommitHook(logrus.StandardLogger(), "origin", "mydb", RolePrimary, func(context.Context) (*doltdb.DoltDB, error) {
		return destEnv.DoltDB, nil
	}, srcEnv.DoltDB, t.TempDir())

	require.False(t, hook.isCaughtUp())
}
