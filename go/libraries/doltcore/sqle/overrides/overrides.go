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

package overrides

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"
)

// EngineOverridesFromContext returns the sql.EngineOverrides from the database provider by looking through the
// context's session. If the context does not have a Dolt session, then the default overrides are returned.
var EngineOverridesFromContext func(ctx context.Context) sql.EngineOverrides

// SchemaFormatterFromContext returns the sql.SchemaFormatter from the overrides obtained from the database provider by
// looking through the context's session. If the context does not have a Dolt session, then the default schema formatter
// is returned.
func SchemaFormatterFromContext(ctx context.Context) sql.SchemaFormatter {
	return sql.GetSchemaFormatter(EngineOverridesFromContext(ctx))
}

// ParserFromContext returns the sql.Parser from the overrides obtained from the database provider by looking through
// the context's session. If the context does not have a Dolt session, then the default parser is returned.
func ParserFromContext(ctx context.Context) sql.Parser {
	return sql.GetParser(EngineOverridesFromContext(ctx))
}
