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

package typed

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
)

func TypedSchemaUnion(schemas ...schema.Schema) (schema.Schema, error) {
	var allCols []schema.Column

	for _, sch := range schemas {
		sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
			allCols = append(allCols, col)
			return false
		})
	}

	allColColl, err := schema.NewColCollection(allCols...)

	if err != nil {
		return nil, err
	}

	sch := schema.SchemaFromCols(allColColl)
	return sch, nil
}
