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

package dsess

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/overrides"
)

func init() {
	// Due to import cycles, we need to set the function to a separate package, since this package is referenced in too
	// many locations. We also can't have this in a higher-level package (such as sqle) due to import cycles once more.
	overrides.EngineOverridesFromContext = EngineOverridesFromContext
}

// EngineOverridesFromContext is defined here due to import cycles.
func EngineOverridesFromContext(ctx context.Context) sql.EngineOverrides {
	if ctx == nil {
		return sql.EngineOverrides{}
	}
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok || sqlCtx == nil {
		return sql.EngineOverrides{}
	}
	dsess, ok := sqlCtx.Session.(*DoltSession)
	if !ok || dsess == nil {
		return sql.EngineOverrides{}
	}
	return dsess.provider.EngineOverrides()
}
