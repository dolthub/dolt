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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
)

func TypedSchemaUnion(schemas ...schema.Schema) (schema.Schema, error) {
	var allCols []schema.Column

	for _, sch := range schemas {
		err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			allCols = append(allCols, col)
			return false, nil
		})

		if err != nil {
			return nil, err
		}
	}

	allColColl, err := schema.NewColCollection(allCols...)

	if err != nil {
		return nil, err
	}

	sch := schema.SchemaFromCols(allColColl)
	return sch, nil
}

func TypedColCollUnion(colColls ...*schema.ColCollection) (*schema.ColCollection, error) {
	var allCols []schema.Column

	for _, sch := range colColls {
		err := sch.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			allCols = append(allCols, col)
			return false, nil
		})

		if err != nil {
			return nil, err
		}
	}

	return schema.NewColCollection(allCols...)
}

func TypedColCollectionIntersection(sch1, sch2 schema.Schema) (*schema.ColCollection, error) {
	var inter []schema.Column
	err := sch1.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		_, ok := sch2.GetAllCols().GetByTag(tag); if ok {
			inter = append(inter, col)
		}
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	if len(inter) == 0 {
		return schema.EmptyColColl, nil
	}

	return schema.NewColCollection(inter...)
}

func TypedColCollectionSubtraction(leftSch, rightSch schema.Schema) (*schema.ColCollection, error) {
	var sub []schema.Column
	err := leftSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		_, ok := rightSch.GetAllCols().GetByTag(tag);

		if !ok {
			sub = append(sub, col)
		}
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	if len(sub) == 0 {
		return schema.EmptyColColl, nil
	}

	return schema.NewColCollection(sub...)
}