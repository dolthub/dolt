// Copyright 2020 Liquidata, Inc.
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

package enginetest

import (
	"context"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/go-mysql-server/enginetest"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

type doltHarness struct {
	t       *testing.T
	session *sqle.DoltSession
	mrEnv   env.MultiRepoEnv
}

var _ enginetest.Harness = (*doltHarness)(nil)

func newDoltHarness(t *testing.T) *doltHarness {
	session, err := sqle.NewDoltSession(context.Background(), enginetest.NewBaseSession(), "test", "email@test.com")
	require.NoError(t, err)
	return &doltHarness{
		t:       t,
		session: session,
		mrEnv:   make(env.MultiRepoEnv),
	}
}

// Logic to skip unsupported queries
func (d *doltHarness) SkipQueryTest(query string) bool {
	lowerQuery := strings.ToLower(query)
	return strings.Contains(lowerQuery, "typestable") || // we don't support all the required types
			strings.Contains(lowerQuery, "show full columns") || // we set extra comment info
			lowerQuery == "show variables" || // we set extra variables
			strings.Contains(lowerQuery, "show create table") // we set extra comment info
}

func (d *doltHarness) Parallelism() int {
	return 1
}

func (d *doltHarness) NewContext() *sql.Context {
	return sql.NewContext(
		context.Background(),
		sql.WithSession(d.session),
		sql.WithViewRegistry(sql.NewViewRegistry()),
	)
}

func (d *doltHarness) NewDatabase(name string) sql.Database {
	dEnv := dtestutils.CreateTestEnv()
	root, err := dEnv.WorkingRoot(enginetest.NewContext(d))
	require.NoError(d.t, err)

	d.mrEnv.AddEnv(name, dEnv)
	db := sqle.NewDatabase(name, dEnv.DoltDB, dEnv.RepoState, dEnv.RepoStateWriter())
	err = db.SetRoot(enginetest.NewContext(d).WithCurrentDB(db.Name()), root)
	require.NoError(d.t, err)
	return db
}

func (d *doltHarness) NewTable(db sql.Database, name string, schema sql.Schema) (sql.Table, error) {
	doltDatabase := db.(sqle.Database)
	err := doltDatabase.CreateTable(enginetest.NewContext(d).WithCurrentDB(db.Name()), name, schema)
	if err != nil {
		return nil, err
	}

	table, ok, err := doltDatabase.GetTableInsensitive(enginetest.NewContext(d).WithCurrentDB(db.Name()), name)

	require.NoError(d.t, err)
	require.True(d.t, ok, "table %s not found after creation", name)
	return table, nil
}

