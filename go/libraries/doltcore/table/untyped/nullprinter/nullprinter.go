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

package nullprinter

import (
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const PrintedNull = "<NULL>"

const NullPrintingStage = "null printing"

// NullPrinter is a utility to convert nil values in rows to a string representation.
type NullPrinter struct {
	Sch schema.Schema
	nullStr string
}

// NewNullPrinter returns a new null printer for the schema given, which must be string-typed (untyped).
func NewNullPrinter(sch schema.Schema) *NullPrinter {
	return &NullPrinter{Sch: sch, nullStr: PrintedNull}
}

// NewNullPrinterWithNullString returns a new null printer for the schema given, which must be string-typed, using the
// string given as the value to print for nulls.
func NewNullPrinterWithNullString(sch schema.Schema, nullStr string) *NullPrinter {
	return &NullPrinter{Sch: sch, nullStr: nullStr}
}

// Function to convert any nil values for a row with the schema given to a string representation. Used as the transform
// function in a NamedTransform.
func (np *NullPrinter) ProcessRow(inRow row.Row, props pipeline.ReadableMap) (rowData []*pipeline.TransformedRowResult, badRowDetails string) {
	taggedVals := make(row.TaggedValues)

	_, err := inRow.IterSchema(np.Sch, func(tag uint64, val types.Value) (stop bool, err error) {
		if !types.IsNull(val) {
			taggedVals[tag] = val
		} else {
			taggedVals[tag] = types.String(np.nullStr)
		}

		return false, nil
	})

	if err != nil {
		return nil, err.Error()
	}

	r, err := row.New(inRow.Format(), np.Sch, taggedVals)

	if err != nil {
		return nil, err.Error()
	}

	return []*pipeline.TransformedRowResult{{RowData: r}}, ""
}
