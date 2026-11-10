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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

type basicProvider struct {
	DoltDatabaseProvider
	calls []string
}

func (p *basicProvider) GetRemoteDB(context.Context, *types.NomsBinFormat, env.Remote) (*doltdb.DoltDB, error) {
	p.calls = append(p.calls, "GetRemoteDB")
	return nil, nil
}

func (p *basicProvider) CreateDatabase(*sql.Context, string) error {
	p.calls = append(p.calls, "CreateDatabase")
	return nil
}

type paramsProvider struct {
	basicProvider
}

func (p *paramsProvider) GetRemoteDBFor(context.Context, *doltdb.DoltDB, env.Remote) (*doltdb.DoltDB, error) {
	p.calls = append(p.calls, "GetRemoteDBFor")
	return nil, nil
}

func (p *paramsProvider) CreateDatabaseWithParams(*sql.Context, string, map[string]string) error {
	p.calls = append(p.calls, "CreateDatabaseWithParams")
	return nil
}

func TestProviderParamsHelpers(t *testing.T) {
	ctx := sql.NewEmptyContext()
	local, err := doltdb.LoadDoltDB(ctx, types.Format_DOLT, doltdb.InMemDoltDB, filesys.LocalFS)
	require.NoError(t, err)
	defer local.Close()
	params := map[string]string{"p": "1"}

	basic := &basicProvider{}
	_, err = GetRemoteDBFor(ctx, basic, local, env.NoRemote)
	require.NoError(t, err)
	require.NoError(t, CreateDatabaseWithParams(ctx, basic, "db", nil))
	assert.Error(t, CreateDatabaseWithParams(ctx, basic, "db", params), "params are not dropped")
	assert.Equal(t, []string{"GetRemoteDB", "CreateDatabase"}, basic.calls)

	full := &paramsProvider{}
	_, err = GetRemoteDBFor(ctx, full, local, env.NoRemote)
	require.NoError(t, err)
	require.NoError(t, CreateDatabaseWithParams(ctx, full, "db", params))
	assert.Equal(t, []string{"GetRemoteDBFor", "CreateDatabaseWithParams"}, full.calls)
}
