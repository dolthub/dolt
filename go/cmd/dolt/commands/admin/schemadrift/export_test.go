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
	"github.com/dolthub/dolt/go/store/val"
)

// ScanForDriftForTest is an exported test helper that drives the same
// drift-scan pipeline the CLI uses. It exists only in the test binary so
// cross-package integration tests can exercise scanForDrift without going
// through Exec().
func ScanForDriftForTest(ctx context.Context, dEnv *env.DoltEnv) ([]DriftRow, error) {
	return scanForDrift(ctx, dEnv)
}

// RepairColumnForTest is the test-side hook into the repair pipeline. Same
// rationale as ScanForDriftForTest: it routes around the CLI wrapper so the
// integration test can assert on the structured RepairResult.
func RepairColumnForTest(ctx context.Context, dEnv *env.DoltEnv, tableName, colName string) (RepairResult, error) {
	return repairColumn(ctx, dEnv, tableName, colName)
}

// RepairColumnWithIncludeEmptyForTest exposes the include-empty repair path
// added earlier. It accepts the homogeneous-empty bucket as a valid repair
// target (when the third argument is true) and refuses it otherwise. Tests
// use this to assert both sides of the default-conservative / opt-in posture.
func RepairColumnWithIncludeEmptyForTest(ctx context.Context, dEnv *env.DoltEnv, tableName, colName string, includeEmpty bool) (RepairResult, error) {
	return repairColumnWithOptions(ctx, dEnv, tableName, colName, includeEmpty)
}

// RecoverRowsColumnForTest is the test-side hook into the recover-rows
// pipeline. Routes around the CLI wrapper so the integration test can assert
// on the structured RecoverRowsResult.
func RecoverRowsColumnForTest(ctx context.Context, dEnv *env.DoltEnv, tableName, colName string) (RecoverRowsResult, error) {
	return recoverRowsColumn(ctx, dEnv, tableName, colName)
}

// ResolveFieldToLegacyForTest exposes the per-row classifier+rewrite helper
// for unit tests. Returns the same tuple the inner pipeline uses: the new
// 20-byte legacy field bytes (or nil for NULL), whether the input was already
// canonical legacy, whether the field bytes changed, and any classification
// or chunkstore error.
func ResolveFieldToLegacyForTest(ctx context.Context, b []byte, vs val.ValueStore, cs ChunkPresenceChecker) (newField []byte, isCanonicalLegacy bool, changed bool, err error) {
	return resolveFieldToLegacy(ctx, b, vs, cs)
}
