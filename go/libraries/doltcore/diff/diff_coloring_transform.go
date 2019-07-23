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

package diff

import (
	"github.com/fatih/color"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
)

var greenTextProp = map[string]interface{}{colorRowProp: color.GreenString}
var redTextProp = map[string]interface{}{colorRowProp: color.RedString}
var yellowTextProp = map[string]interface{}{colorRowProp: color.YellowString}

// Unused, color logic moved to ColorDiffSink. Still handy.
func ColoringTransform(r row.Row, props pipeline.ReadableMap) ([]*pipeline.TransformedRowResult, string) {
	var updatedProps map[string]interface{}
	diffType, ok := props.Get(DiffTypeProp)

	if ok {
		ct, ok := diffType.(DiffChType)

		if ok {
			switch ct {
			case DiffAdded:
				updatedProps = greenTextProp
			case DiffRemoved:
				updatedProps = redTextProp
			case DiffModifiedOld:
				updatedProps = yellowTextProp
			case DiffModifiedNew:
				updatedProps = yellowTextProp
			}
		}
	}

	return []*pipeline.TransformedRowResult{{RowData: r, PropertyUpdates: updatedProps}}, ""
}
