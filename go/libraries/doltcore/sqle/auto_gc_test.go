// Copyright 2025 Dolthub, Inc.
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
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/datas"
)

func TestAutoGCController(t *testing.T) {
	NewLogger := func() *logrus.Logger {
		res := logrus.New()
		res.SetOutput(new(bytes.Buffer))
		return res
	}
	CtxFactory := func(ctx context.Context) (*sql.Context, error) {
		return sql.NewContext(ctx, sql.WithSession(sql.NewBaseSession())), nil
	}
	t.Run("Hook", func(t *testing.T) {
		t.Run("NeverStarted", func(t *testing.T) {
			controller := NewAutoGCController(NewLogger())
			hook := controller.newCommitHook("some_database", nil)
			hook.stop()
		})
		t.Run("StartedBeforeNewHook", func(t *testing.T) {
			controller := NewAutoGCController(NewLogger())
			bg := sql.NewBackgroundThreads()
			defer bg.Shutdown()
			err := controller.RunBackgroundThread(bg, CtxFactory)
			require.NoError(t, err)
			ctx := context.Background()
			dEnv := CreateTestEnvWithName("some_database")
			hook := controller.newCommitHook("some_database", dEnv.DoltDB(ctx))
			hook.Execute(ctx, datas.Dataset{}, nil)
			hook.stop()
		})
		t.Run("StartedAfterNewHook", func(t *testing.T) {
			controller := NewAutoGCController(NewLogger())
			bg := sql.NewBackgroundThreads()
			defer bg.Shutdown()
			ctx := context.Background()
			dEnv := CreateTestEnvWithName("some_database")
			hook := controller.newCommitHook("some_database", dEnv.DoltDB(ctx))
			err := controller.RunBackgroundThread(bg, CtxFactory)
			require.NoError(t, err)
			hook.Execute(ctx, datas.Dataset{}, nil)
			hook.stop()
		})
		t.Run("ExecuteOnCanceledCtx", func(t *testing.T) {
			controller := NewAutoGCController(NewLogger())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			dEnv := CreateTestEnvWithName("some_database")
			hook := controller.newCommitHook("some_database", dEnv.DoltDB(ctx))
			_, err := hook.Execute(ctx, datas.Dataset{}, nil)
			require.ErrorIs(t, err, context.Canceled)
		})
	})
	t.Run("gcBgThread", func(t *testing.T) {
		controller := NewAutoGCController(NewLogger())
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			controller.gcBgThread(ctx)
		}()
		time.Sleep(50 * time.Millisecond)
		cancel()
		wg.Wait()
	})
	t.Run("DatabaseProviderHooks", func(t *testing.T) {
		t.Run("Unstarted", func(t *testing.T) {
			controller := NewAutoGCController(NewLogger())
			ctx, err := CtxFactory(context.Background())
			require.NoError(t, err)
			dEnv := CreateTestEnvWithName("some_database")
			err = controller.InitDatabaseHook()(ctx, nil, "some_database", dEnv, nil)
			require.NoError(t, err)
			controller.DropDatabaseHook()(nil, "some_database")
		})
		t.Run("Started", func(t *testing.T) {
			controller := NewAutoGCController(NewLogger())
			bg := sql.NewBackgroundThreads()
			defer bg.Shutdown()
			err := controller.RunBackgroundThread(bg, CtxFactory)
			require.NoError(t, err)
			ctx, err := CtxFactory(context.Background())
			require.NoError(t, err)
			dEnv := CreateTestEnvWithName("some_database")
			err = controller.InitDatabaseHook()(ctx, nil, "some_database", dEnv, nil)
			require.NoError(t, err)
			controller.DropDatabaseHook()(nil, "some_database")
		})
	})
}
