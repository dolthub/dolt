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

package dtablefunctions

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/val"
)

// IsNaturallyOrdered reports whether a direct primary-tree diff scan satisfies
// the requested effective-key ordering. It must not be used for indexed access:
// secondary indexes and concatenated lookup ranges have different ordering.
func (dtf *DiffTableFunction) IsNaturallyOrdered(ctx *sql.Context, order sql.SortConditions, source string) bool {
	from, to := dtf.tableDelta.FromSch, dtf.tableDelta.ToSch
	if dtf.overriddenSchema != nil || from == nil || to == nil ||
		schema.IsKeyless(from) || schema.IsKeyless(to) || !schema.ArePrimaryKeySetsDiffable(from, to) {
		return false
	}
	keys := to.GetPKCols()
	// Require the complete key, giving pagination a total order.
	if len(order) != keys.Size() {
		return false
	}
	fromDesc, toDesc := from.GetKeyDescriptor(nil), to.GetKeyDescriptor(nil)
	if !fromDesc.Equals(toDesc) {
		return false
	}
	for i, condition := range order {
		if fromDesc.Types[i].Nullable || toDesc.Types[i].Nullable {
			return false
		}
		if condition.Order != sql.Ascending || fromDesc.Comparator().Order(i).Descending || toDesc.Comparator().Order(i).Descending {
			return false
		}
		// Only encodings whose physical order is the SQL value order are eligible.
		switch toDesc.Types[i].Enc {
		case val.Int8Enc, val.Uint8Enc, val.Int16Enc, val.Uint16Enc, val.Int32Enc, val.Uint32Enc,
			val.Int64Enc, val.Uint64Enc, val.Float32Enc, val.Float64Enc, val.StringEnc, val.ByteStringEnc,
			val.Bit64Enc, val.YearEnc, val.DateEnc, val.TimeEnc, val.DatetimeEnc, val.DecimalEnc:
		default:
			return false
		}
		coalesce, ok := condition.Expr.(*function.Coalesce)
		if !ok || len(coalesce.Children()) != 2 {
			return false
		}
		fromCol, toCol := from.GetPKCols().GetByIndex(i), keys.GetByIndex(i)
		for j, name := range []string{"to_" + toCol.Name, "from_" + fromCol.Name} {
			field, ok := coalesce.Children()[j].(*expression.GetField)
			if !ok || !strings.EqualFold(field.Table(), source) || !strings.EqualFold(field.Name(), name) {
				return false
			}
			idx := field.Index()
			if idx < 0 || idx >= len(dtf.sqlSch) || !strings.EqualFold(dtf.sqlSch[idx].Name, name) {
				return false
			}
		}
		if !coalesce.Type(ctx).Equals(toCol.TypeInfo.ToSqlType()) {
			return false
		}
	}
	return true
}
