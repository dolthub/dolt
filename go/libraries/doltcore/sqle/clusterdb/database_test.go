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

package clusterdb

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterDatabaseEventsDoNotError asserts that the dolt_cluster database does not cause the
// event scheduler to log:
//
//	"unable to determine if events need to be reloaded: database 'dolt_cluster' doesn't support events"
//
// The event executor iterates the catalog's databases, each wrapped in a
// mysql_db.PrivilegedDatabase. PrivilegedDatabase exposes the sql.EventDatabase interface
// unconditionally, returning sql.ErrEventsNotSupported when the underlying database does not
// implement events. That error is logged (and short-circuits the reload check for all databases
// iterated after dolt_cluster, and disables event-executor quiescing). The cluster database must
// therefore implement sql.EventDatabase as a no-op so NeedsToReloadEvents returns (false, nil).
func TestClusterDatabaseEventsDoNotError(t *testing.T) {
	db := NewClusterDatabase(nil)

	// The cluster database must implement sql.EventDatabase as a no-op. Without this, the
	// PrivilegedDatabase wrapper returns ErrEventsNotSupported for dolt_cluster.
	edb, ok := db.(sql.EventDatabase)
	require.True(t, ok, "cluster database must implement sql.EventDatabase so the event scheduler does not error")

	needsReload, err := edb.NeedsToReloadEvents(nil, nil)
	require.NoError(t, err)
	assert.False(t, needsReload)

	events, _, err := edb.GetEvents(nil)
	require.NoError(t, err)
	assert.Empty(t, events)

	// Reproduce the exact path the event executor takes: it wraps databases in a
	// PrivilegedDatabase, which is the actual source of the ErrEventsNotSupported error seen in
	// the logs. NeedsToReloadEvents must not return an error (that error is what gets logged).
	pdb := mysql_db.NewPrivilegedDatabase(nil, db, nil)

	pedb, ok := pdb.(sql.EventDatabase)
	require.True(t, ok)
	needsReload, err = pedb.NeedsToReloadEvents(nil, nil)
	require.NoError(t, err, "PrivilegedDatabase wrapping dolt_cluster must not error on NeedsToReloadEvents")
	assert.False(t, needsReload)

	// dolt_cluster must not prevent the event executor from quiescing when there are no events.
	qedb, ok := pdb.(sql.QuiescableEventDatabase)
	require.True(t, ok)
	assert.True(t, qedb.QuiescableEvents(), "dolt_cluster must not block event-executor quiescing")
}
