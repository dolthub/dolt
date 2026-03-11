// Copyright 2019 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/valutil"
	"github.com/dolthub/dolt/go/store/types"
)

// Row is the original row interface used by noms valued rows. It's still used in some test code and in a few legacy
// command line reader interfaces, but should not be used for new code. Use |val.Tuple| to express rows.
// Deprecated
type Row interface {
	// IterSchema iterates over all columns in the schema, using the value for the row. Columns that have no value set
	// in this row will still be visited, and receive a nil value.
	IterSchema(sch schema.Schema, cb func(tag uint64, val types.Value) (stop bool, err error)) (bool, error)

	// GetColVal returns the value for the column with the tag given, and a success bool. The value will be null if the
	// row doesn't contain a value for that tag.
	GetColVal(tag uint64) (types.Value, bool)
}

func New(nbf *types.NomsBinFormat, sch schema.Schema, colVals TaggedValues) (Row, error) {
	if schema.IsKeyless(sch) {
		return keylessRowFromTaggedValued(nbf, sch, colVals)
	}
	return pkRowFromTaggedValues(nbf, sch, colVals)
}

func AreEqual(row1, row2 Row, sch schema.Schema) bool {
	if row1 == nil && row2 == nil {
		return true
	} else if row1 == nil || row2 == nil {
		return false
	}

	for _, tag := range sch.GetAllCols().Tags {
		val1, _ := row1.GetColVal(tag)
		val2, _ := row2.GetColVal(tag)

		if !valutil.NilSafeEqCheck(val1, val2) {
			return false
		}
	}

	return true
}
