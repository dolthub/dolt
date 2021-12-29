// Copyright 2021 Dolthub, Inc.
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

package writer

import (
	"context"
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// todo(andy): cleanup
func debugIndexes(r *doltdb.RootValue) string {
	if r == nil {
		return ""
	}

	ctx := context.Background()
	sb := strings.Builder{}

	_ = r.IterTables(ctx, func(name string, table *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		sb.WriteString("table: ")
		sb.WriteString(name)
		sb.WriteRune('\n')

		pk, _ := table.GetRowData(ctx)
		if pk.Len() > 0 {
			sz := strconv.Itoa(int(pk.Len()))
			sb.WriteRune('\t')
			sb.WriteString("primary: ")
			sb.WriteString(sz)
			sb.WriteRune('\n')
		}

		id, _ := table.GetIndexData(ctx)
		_ = id.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
			idx, _ := value.(types.Ref).TargetValue(ctx, table.ValueReadWriter())
			idxName := string(key.(types.String))
			l := int(idx.(types.Map).Len())

			if l > 0 {
				sz := strconv.Itoa(l)
				sb.WriteRune('\t')
				sb.WriteString(idxName)
				sb.WriteString(": ")
				sb.WriteString(sz)
				sb.WriteRune('\n')
			}

			return false, nil
		})

		return false, nil
	})

	return sb.String()
}
