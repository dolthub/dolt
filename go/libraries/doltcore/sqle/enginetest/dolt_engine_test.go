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
	"testing"

	"github.com/liquidata-inc/go-mysql-server/enginetest"
)

func TestQueries(t *testing.T) {
	enginetest.TestQueries(t, newDoltHarness(t))
}

func TestVersionedQueries(t *testing.T) {
	enginetest.TestVersionedQueries(t, newDoltHarness(t))
}

// Tests of choosing the correct execution plan independent of result correctness. Mostly useful for confirming that
// the right indexes are being used for joining tables.
func TestQueryPlans(t *testing.T) {
	t.Skipf("Skipping query plan tests until we have unified printing for query plans")
	enginetest.TestQueryPlans(t, newDoltHarness(t))
}

func TestQueryErrors(t *testing.T) {
	enginetest.TestQueryErrors(t, newDoltHarness(t))
}

func TestInfoSchema(t *testing.T) {
	enginetest.TestInfoSchema(t, newDoltHarness(t))
}

func TestColumnAliases(t *testing.T) {
	enginetest.TestColumnAliases(t, newDoltHarness(t))
}

func TestOrderByGroupBy(t *testing.T) {
	enginetest.TestOrderByGroupBy(t, newDoltHarness(t))
}

func TestAmbiguousColumnResolution(t *testing.T) {
	enginetest.TestAmbiguousColumnResolution(t, newDoltHarness(t))
}

func TestInsertInto(t *testing.T) {
	enginetest.TestInsertInto(t, newDoltHarness(t))
}

func TestInsertIntoErrors(t *testing.T) {
	enginetest.TestInsertIntoErrors(t, newDoltHarness(t))
}

func TestReplaceInto(t *testing.T) {
	t.Skipf("Skipping, replace returns the wrong number of rows in some cases")
	enginetest.TestReplaceInto(t, newDoltHarness(t))
}

func TestReplaceIntoErrors(t *testing.T) {
	enginetest.TestReplaceIntoErrors(t, newDoltHarness(t))
}

func TestUpdate(t *testing.T) {
	enginetest.TestUpdate(t, newDoltHarness(t))
}

func TestUpdateErrors(t *testing.T) {
	enginetest.TestUpdateErrors(t, newDoltHarness(t))
}

func TestDeleteFrom(t *testing.T) {
	enginetest.TestDelete(t, newDoltHarness(t))
}

func TestDeleteFromErrors(t *testing.T) {
	enginetest.TestDeleteErrors(t, newDoltHarness(t))
}

func TestCreateTable(t *testing.T) {
	t.Skipf("Skipping: no support for BLOB type")
	enginetest.TestCreateTable(t, newDoltHarness(t))
}

func TestDropTable(t *testing.T) {
	enginetest.TestDropTable(t, newDoltHarness(t))
}

func TestRenameTable(t *testing.T) {
	enginetest.TestRenameTable(t, newDoltHarness(t))
}

func TestRenameColumn(t *testing.T) {
	t.Skipf("DDL tests break because of column comments")
	enginetest.TestRenameColumn(t, newDoltHarness(t))
}

func TestAddColumn(t *testing.T) {
	t.Skipf("DDL tests break because of column comments")
	enginetest.TestAddColumn(t, newDoltHarness(t))
}

func TestModifyColumn(t *testing.T) {
	t.Skip("Type changes aren't supported")
	enginetest.TestModifyColumn(t, newDoltHarness(t))
}

func TestDropColumn(t *testing.T) {
	t.Skipf("DDL tests break because of column comments")
	enginetest.TestDropColumn(t, newDoltHarness(t))
}

func TestCreateForeignKeys(t *testing.T) {
	enginetest.TestCreateForeignKeys(t, newDoltHarness(t))
}

func TestDropForeignKeys(t *testing.T) {
	enginetest.TestDropForeignKeys(t, newDoltHarness(t))
}

func TestExplode(t *testing.T) {
	t.Skipf("Unsupported types")
	enginetest.TestExplode(t, newDoltHarness(t))
}

func TestReadOnly(t *testing.T) {
	enginetest.TestReadOnly(t, newDoltHarness(t))
}

func TestViews(t *testing.T) {
	enginetest.TestViews(t, newDoltHarness(t))
}

func TestVersionedViews(t *testing.T) {
	enginetest.TestVersionedViews(t, newDoltHarness(t))
}

func TestNaturalJoin(t *testing.T) {
	enginetest.TestNaturalJoin(t, newDoltHarness(t))
}

func TestNaturalJoinEqual(t *testing.T) {
	enginetest.TestNaturalJoinEqual(t, newDoltHarness(t))
}

func TestNaturalJoinDisjoint(t *testing.T) {
	enginetest.TestNaturalJoinDisjoint(t, newDoltHarness(t))
}

func TestInnerNestedInNaturalJoins(t *testing.T) {
	t.Skip("No primary key in test tables")
	enginetest.TestInnerNestedInNaturalJoins(t, newDoltHarness(t))
}
