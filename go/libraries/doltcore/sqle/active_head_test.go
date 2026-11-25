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

package sqle

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// TestActiveHeadFromOtherSession reads a session's active head from another session while it checks out branches and
// switches databases. Run it with -race.
func TestActiveHeadFromOtherSession(t *testing.T) {
	ctx := context.Background()
	dEnv := CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()

	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)
	engine, ownerCtx, err := NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)
	exec := func(q string) error {
		_, iter, _, err := engine.Query(ownerCtx, q)
		if err != nil {
			return err
		}
		return drainIter(ownerCtx, iter)
	}

	owner := dsess.DSessFromSess(ownerCtx.Session)
	config, _ := dEnv.Config.GetConfig(env.GlobalConfig)
	otherCtx := NewTestSQLCtxWithProvider(ctx, owner.Provider(), config, nil, nil, owner.EngineVariables())

	// Before the owner loads the database, its head is the default one.
	head, ok, err := owner.ActiveHead(otherCtx, "dolt")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "main", head)
	_, ok, err = owner.CurrentHead(ownerCtx, "dolt")
	require.NoError(t, err)
	require.False(t, ok, "reading the active head loaded the database into the owner")

	require.NoError(t, exec("call dolt_branch('b')"))

	var done atomic.Bool
	var eg errgroup.Group
	eg.Go(func() error {
		defer done.Store(true)
		for i := 0; i < 50; i++ {
			for _, q := range []string{
				"call dolt_checkout('b')",
				"call dolt_checkout('main')",
				"use `dolt/b`",
				"use dolt",
				fmt.Sprintf("call dolt_branch('tmp%d')", i),
				fmt.Sprintf("call dolt_branch('-d', 'tmp%d')", i),
			} {
				if err := exec(q); err != nil {
					return fmt.Errorf("%s: %w", q, err)
				}
			}
		}
		return nil
	})
	var heads []string
	eg.Go(func() error {
		for !done.Load() {
			head, ok, err := owner.ActiveHead(otherCtx, "dolt")
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("owner is not on dolt")
			}
			heads = append(heads, head)
		}
		return nil
	})
	require.NoError(t, eg.Wait())
	for _, head := range heads {
		assert.Contains(t, []string{"main", "b"}, head)
	}

	_, ok, err = owner.ActiveHead(otherCtx, "other")
	require.NoError(t, err)
	assert.False(t, ok)
}
