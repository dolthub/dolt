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

package dsess

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/variables"
	"github.com/stretchr/testify/require"
)

func TestSequenceTrackerLockModeUsesOwningRegistry(t *testing.T) {
	a, b := variables.NewEngineVariables(), variables.NewEngineVariables()
	ctxA := sql.NewContextWithVariables(context.Background(), a, sql.WithSession(DefaultSessionWithVariables(emptyDatabaseProvider(), nil, a)))
	ctxB := sql.NewContextWithVariables(context.Background(), b, sql.WithSession(DefaultSessionWithVariables(emptyDatabaseProvider(), nil, b)))
	require.NoError(t, a.System.(sql.ContextAwareSystemVariableRegistry).AssignValuesWithContext(ctxA, map[string]interface{}{"innodb_autoinc_lock_mode": int64(0)}))
	require.NoError(t, b.System.(sql.ContextAwareSystemVariableRegistry).AssignValuesWithContext(ctxB, map[string]interface{}{"innodb_autoinc_lock_mode": int64(2)}))

	require.Equal(t, LockMode(0), currentLockMode(ctxA))
	require.Equal(t, LockMode(2), currentLockMode(ctxB))

	trackerA, err := NewAutoIncrementTracker(ctxA, "db")
	require.NoError(t, err)
	defer trackerA.Close()
	trackerB, err := NewAutoIncrementTracker(ctxB, "db")
	require.NoError(t, err)
	defer trackerB.Close()
	require.NoError(t, trackerA.awaitInit(ctxA))
	require.NoError(t, trackerB.awaitInit(ctxB))
	require.Equal(t, LockMode(0), trackerA.lockMode)
	require.Equal(t, LockMode(2), trackerB.lockMode)

	require.NoError(t, a.System.(sql.ContextAwareSystemVariableRegistry).AssignValuesWithContext(ctxA, map[string]interface{}{"innodb_autoinc_lock_mode": int64(1)}))
	require.Equal(t, LockMode(0), trackerA.lockMode)
}
