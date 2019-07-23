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

package resultset

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

// For most analysis, rows can be considered independent of their schema (schemas just provide type / tag number
// validation). This falls apart when we need to combine rows from multiple tables together into a new result set that
// is some combination of the source schemas. For these use cases, it becomes very useful to package row info with the
// schema of the table it came from.
type RowWithSchema struct {
	Row    row.Row
	Schema schema.Schema
}

// Updates the column value. Unlike Row, RowWithSchema is mutable. Calling this method updates the underlying row.
func (r *RowWithSchema) SetColVal(tag uint64, value types.Value) error {
	newRow, err := r.Row.SetColVal(tag, value, r.Schema)
	if err != nil {
		return err
	}
	r.Row = newRow
	return nil
}

// Returns the underlying column value for the tag given. Convenience method for calling GetColVal on the underlying
// row field.
func (r *RowWithSchema) GetColVal(tag uint64) (types.Value, bool) {
	return r.Row.GetColVal(tag)
}

// Returns a copy of this row.
func (r *RowWithSchema) Copy() RowWithSchema {
	return RowWithSchema{r.Row, r.Schema}
}
