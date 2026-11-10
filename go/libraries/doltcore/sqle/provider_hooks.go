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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/types"
)

// evictCachedGitRemote closes and forgets the git remote that |p| caches under |key| if it cannot serve a database
// opened with |params|. Every cached git remote can serve any database in Dolt.
var evictCachedGitRemote = func(p *DoltDatabaseProvider, ctx context.Context, key string, params map[string]interface{}) {}

// openPushOnWriteRemote opens remote |r|, which the commits of |dEnv|'s database are pushed to as they are written.
var openPushOnWriteRemote = func(ctx context.Context, r env.Remote, dEnv *env.DoltEnv) (*doltdb.DoltDB, error) {
	return r.GetRemoteDB(ctx, types.Format_DOLT, dEnv)
}

// createClientDatabase creates database |name| that a remotes API client asked for with |ctx|.
var createClientDatabase = func(ctx context.Context, sqlCtx *sql.Context, p dsess.DoltDatabaseProvider, name string) error {
	return p.CreateDatabase(sqlCtx, name)
}
