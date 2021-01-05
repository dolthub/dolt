// Copyright 2020 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

func init() {
	sqle.MinRowsPerPartition = 2
}

func limitTestQueriesTo(queries ...string) {
	querySet := set.NewStrSet(queries)

	var broken []enginetest.QueryTest
	for _, t := range enginetest.QueryTests {
		if querySet.Contains(t.Query) {
			broken = append(broken, t)
		}
	}

	enginetest.QueryTests = broken
}

func TestQueries(t *testing.T) {
	enginetest.TestQueries(t, newDoltHarness(t))
}

func TestSingleQuery(t *testing.T) {
	t.Skip()

	var test enginetest.QueryTest
	test = enginetest.QueryTest{
		Query: `SELECT i FROM mytable mt 
						 WHERE (SELECT i FROM mytable where i = mt.i and i > 2) IS NOT NULL
						 AND (SELECT i2 FROM othertable where i2 = i) IS NOT NULL
						 ORDER BY i`,
		Expected: []sql.Row{
			{3},
		},
	}

	harness := newDoltHarness(t)
	engine := enginetest.NewEngine(t, harness)
	engine.Analyzer.Debug = true
	engine.Analyzer.Verbose = true

	enginetest.TestQuery(t, harness, engine, test.Query, test.Expected, nil)
}

func TestVersionedQueries(t *testing.T) {
	enginetest.TestVersionedQueries(t, newDoltHarness(t))
}

// Tests of choosing the correct execution plan independent of result correctness. Mostly useful for confirming that
// the right indexes are being used for joining tables.
func TestQueryPlans(t *testing.T) {
	// TODO: FIX THESE TESTS!!!
	skipped := set.NewStrSet([]string{
		"SELECT * FROM mytable mt INNER JOIN othertable ot ON mt.i = ot.i2 AND mt.i > 2",
		"SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1",
		"SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1 ORDER BY 1",
		"SELECT pk,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2",
		`SELECT i FROM mytable mt
		WHERE (SELECT i FROM mytable where i = mt.i and i > 2) IS NOT NULL
		AND (SELECT i2 FROM othertable where i2 = i) IS NOT NULL`,
		"SELECT pk,pk2, (SELECT pk from one_pk where pk = 1 limit 1) FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2",
		"SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1",
		"SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i AND f IS NOT NULL",
		"SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0",
		"SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
		"SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3",
		"SELECT opk.c5,pk1,pk2 FROM one_pk opk, two_pk tpk WHERE pk=pk1 ORDER BY 1,2,3",
		"SELECT one_pk.c5,pk1,pk2 FROM one_pk,two_pk WHERE pk=pk1 ORDER BY 1,2,3",
		"SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0 ORDER BY 2,3",
		"SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk1-pk>0 AND pk2<1",
		"SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ORDER BY 1,2,3",
		"SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3",
		"SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE one_pk.c1=two_pk.c1 ORDER BY 1,2,3",
		"SELECT pk,pk1,pk2,one_pk.c1 AS foo, two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 ORDER BY 1,2,3",
		"SELECT pk,pk1,pk2,one_pk.c1 AS foo,two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE one_pk.c1=10",
	})

	tests := make([]enginetest.QueryPlanTest, 0, len(enginetest.PlanTests))
	for _, currTest := range enginetest.PlanTests {
		if !skipped.Contains(currTest.Query) {
			tests = append(tests, currTest)
		}
	}
	enginetest.PlanTests = tests

	// Parallelism introduces Exchange nodes into the query plans, so disable.
	// TODO: exchange nodes should really only be part of the explain plan under certain debug settings
	enginetest.TestQueryPlans(t, newDoltHarness(t).WithParallelism(1))
}

func TestQueryErrors(t *testing.T) {
	enginetest.TestQueryErrors(t, newDoltHarness(t))
}

func TestInfoSchema(t *testing.T) {
	enginetest.TestInfoSchema(t, newDoltHarness(t))
}

func TestColumnAliases(t *testing.T) {
	assert.Equal(t, 1, 1, "msg", 1, 2)
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

func TestTruncate(t *testing.T) {
	enginetest.TestTruncate(t, newDoltHarness(t))
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

func TestColumnDefaults(t *testing.T) {
	enginetest.TestColumnDefaults(t, newDoltHarness(t))
}

func TestTriggers(t *testing.T) {
	enginetest.TestTriggers(t, newDoltHarness(t))
}
