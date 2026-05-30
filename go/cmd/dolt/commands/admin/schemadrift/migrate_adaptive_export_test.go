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

package schemadrift

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

// MigrateAdaptiveColumnForTest is the test-side hook into the forward
// migrate-adaptive pipeline . Mirrors RepairColumnForTest /
// RecoverRowsColumnForTest so cross-package tests can assert on the structured
// MigrateAdaptiveResult without going through Exec().
//
// Part of this test suite (added in a dedicated file so it does not
// clash with later edits to export_test.go).
func MigrateAdaptiveColumnForTest(ctx context.Context, dEnv *env.DoltEnv, tableName, colName string) (MigrateAdaptiveResult, error) {
	return migrateAdaptiveColumn(ctx, dEnv, tableName, colName)
}
