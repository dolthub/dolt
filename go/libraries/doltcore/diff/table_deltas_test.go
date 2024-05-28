// Copyright 2022 Dolthub, Inc.
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

package diff

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

var sch = schema.MustSchemaFromCols(schema.NewColCollection(
	schema.NewColumn("pk", 0, types.StringKind, false),
))
var sch2 = schema.MustSchemaFromCols(schema.NewColCollection(
	schema.NewColumn("pk2", 1, types.StringKind, false),
))
var sch3 = schema.MustSchemaFromCols(schema.NewColCollection(
	schema.NewColumn("pk3", 2, types.StringKind, false),
))
var sch4 = schema.MustSchemaFromCols(schema.NewColCollection(
	schema.NewColumn("pk4", 3, types.StringKind, false),
))
var sch5 = schema.MustSchemaFromCols(schema.NewColCollection(
	schema.NewColumn("pk5", 4, types.StringKind, false),
))

func TestMatchTableDeltas(t *testing.T) {
	var fromDeltas = []TableDelta{
		{FromName: doltdb.TableName{Name: "should_match_on_name"}, FromSch: sch},
		{FromName: doltdb.TableName{Name: "dropped"}, FromSch: sch},
		{FromName: doltdb.TableName{Name: "dropped2"}, FromSch: sch3},
		{FromName: doltdb.TableName{Name: "renamed_before"}, FromSch: sch5},
	}
	var toDeltas = []TableDelta{
		{ToName: doltdb.TableName{Name: "should_match_on_name"}, ToSch: sch},
		{ToName: doltdb.TableName{Name: "added"}, ToSch: sch2},
		{ToName: doltdb.TableName{Name: "added2"}, ToSch: sch4},
		{ToName: doltdb.TableName{Name: "renamed_after"}, ToSch: sch5},
	}
	expected := []TableDelta{
		{FromName: doltdb.TableName{Name: "should_match_on_name"}, ToName: doltdb.TableName{Name: "should_match_on_name"}, FromSch: sch, ToSch: sch},
		{FromName: doltdb.TableName{Name: "renamed_before"}, ToName: doltdb.TableName{Name: "renamed_after"}, FromSch: sch5, ToSch: sch5},
		{FromName: doltdb.TableName{Name: "dropped"}, FromSch: sch},
		{FromName: doltdb.TableName{Name: "dropped2"}, FromSch: sch3},
		{ToName: doltdb.TableName{Name: "added"}, ToSch: sch2},
		{ToName: doltdb.TableName{Name: "added2"}, ToSch: sch4},
	}

	for i := 0; i < 100; i++ {
		received := matchTableDeltas(fromDeltas, toDeltas)
		require.ElementsMatch(t, expected, received)
	}
}
