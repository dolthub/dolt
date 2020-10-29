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

package fwt

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
)

// TooLongBehavior determines how the FWTTransformer should behave when it encounters a column that is longer than what
// it expected
type TooLongBehavior int

const (
	// ErrorWhenTooLong treats each row containing a column that is longer than expected as a bad row
	ErrorWhenTooLong TooLongBehavior = iota
	//SkipRowWhenTooLong caller can use ErrorWhenTooLong and ignore rows that return ErrColumnTooLong
	// TruncateWhenTooLong will cut off the end of columns that are too long
	TruncateWhenTooLong
	// HashFillWhenTooLong will result in ######### being printed in place of the columns that are longer than expected.
	HashFillWhenTooLong
	// PrintAllWhenTooLong will print the entire column for every row.  When this happens results will not be valid
	// fixed width text files
	PrintAllWhenTooLong
)

// FWTTransformer transforms columns to be of fixed width.
type FWTTransformer struct {
	sch       schema.Schema
	formatter FixedWidthFormatter
}

// NewFWTTransform creates a new FWTTransformer from a FWTSchema and a TooLongBehavior
func NewFWTTransformer(sch schema.Schema, fwf FixedWidthFormatter) *FWTTransformer {
	return &FWTTransformer{sch, fwf}
}

// Transform takes in a row and transforms it so that it's columns are of the correct width.
func (fwtTr *FWTTransformer) Transform(r row.Row, props pipeline.ReadableMap) ([]*pipeline.TransformedRowResult, string) {
	r, err := fwtTr.formatter.FormatRow(r, fwtTr.sch)

	if err != nil {
		return nil, err.Error()
	}

	return []*pipeline.TransformedRowResult{{RowData: r}}, ""
}
