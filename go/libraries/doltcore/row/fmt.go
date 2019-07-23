// Copyright 2019 Liquidata, Inc.
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

package row

import (
	"bytes"
	"context"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

type RowFormatFunc func(ctx context.Context, r Row, sch schema.Schema) string

var Fmt = FieldSeparatedFmt(':')
var fieldDelim = []byte(" | ")

func FieldSeparatedFmt(delim rune) RowFormatFunc {
	return func(ctx context.Context, r Row, sch schema.Schema) string {
		if r == nil {
			return "null"
		}

		allCols := sch.GetAllCols()
		kvps := make([]string, 0, allCols.Size())

		var backingBuffer [512]byte
		buf := bytes.NewBuffer(backingBuffer[:0])

		var ok bool
		allCols.IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
			if ok {
				buf.Write(fieldDelim)
			}

			var val types.Value
			val, ok = r.GetColVal(tag)

			if ok {
				buf.Write([]byte(col.Name))
				buf.WriteRune(delim)
				types.WriteEncodedValue(ctx, buf, val)
				kvps = append(kvps, buf.String())
			}

			return false
		})

		return buf.String()
	}
}
