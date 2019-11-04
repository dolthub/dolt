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

package rowconv

import (
	"errors"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type ColNamingFunc func(colName string) string

type stringUint64Tuple struct {
	str string
	u64 uint64
}

type NamedSchema struct {
	Name string
	Sch  schema.Schema
}

type Joiner struct {
	srcSchemas map[string]schema.Schema
	tagMaps    map[string]map[uint64]uint64
	revTagMap  map[uint64]stringUint64Tuple
	tags       map[string][]uint64
	joined     schema.Schema
}

func NewJoiner(namedSchemas []NamedSchema, namers map[string]ColNamingFunc) (*Joiner, error) {
	tags := make(map[string][]uint64)
	revTagMap := make(map[uint64]stringUint64Tuple)
	tagMaps := make(map[string]map[uint64]uint64, len(namedSchemas))
	srcSchemas := make(map[string]schema.Schema)
	for _, namedSch := range namedSchemas {
		tagMaps[namedSch.Name] = make(map[uint64]uint64)
		srcSchemas[namedSch.Name] = namedSch.Sch
	}

	var cols []schema.Column
	var destTag uint64
	for _, namedSch := range namedSchemas {
		sch := namedSch.Sch
		name := namedSch.Name
		allCols := sch.GetAllCols()
		namer := namers[name]
		err := allCols.Iter(func(srcTag uint64, col schema.Column) (stop bool, err error) {
			newColName := namer(col.Name)
			cols = append(cols, schema.NewColumn(newColName, destTag, col.Kind, false))
			tagMaps[name][srcTag] = destTag
			revTagMap[destTag] = stringUint64Tuple{str: name, u64: srcTag}
			tags[name] = append(tags[name], destTag)
			destTag++

			return false, nil
		})

		if err != nil {
			return nil, err
		}
	}

	colColl, err := schema.NewColCollection(cols...)

	if err != nil {
		return nil, err
	}

	joined := schema.UnkeyedSchemaFromCols(colColl)

	return &Joiner{srcSchemas, tagMaps, revTagMap, tags, joined}, nil
}

func (j *Joiner) Join(namedRows map[string]row.Row) (row.Row, error) {
	var nbf *types.NomsBinFormat
	colVals := make(row.TaggedValues)
	for name, r := range namedRows {
		if r == nil {
			continue
		}

		if nbf == nil {
			nbf = r.Format()
		} else if nbf.VersionString() != r.Format().VersionString() {
			return nil, errors.New("not all rows have the same format")
		}

		_, err := r.IterCols(func(tag uint64, val types.Value) (stop bool, err error) {
			destTag := j.tagMaps[name][tag]
			colVals[destTag] = val

			return false, nil
		})

		if err != nil {
			return nil, err
		}
	}

	return row.New(nbf, j.joined, colVals)
}

func (j *Joiner) Split(r row.Row) (map[string]row.Row, error) {
	colVals := make(map[string]row.TaggedValues, len(j.srcSchemas))
	for name := range j.srcSchemas {
		colVals[name] = make(row.TaggedValues)
	}

	_, err := r.IterCols(func(tag uint64, val types.Value) (stop bool, err error) {
		schemaNameAndTag := j.revTagMap[tag]
		colVals[schemaNameAndTag.str][schemaNameAndTag.u64] = val

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	rows := make(map[string]row.Row, len(colVals))
	for name, sch := range j.srcSchemas {
		var err error

		currColVals := colVals[name]

		if len(currColVals) == 0 {
			continue
		}

		rows[name], err = row.New(r.Format(), sch, currColVals)

		if err != nil {
			return nil, err
		}
	}

	return rows, nil
}

func (j *Joiner) GetSchema() schema.Schema {
	return j.joined
}

func (j *Joiner) TagsForSchema(name string) []uint64 {
	return j.tags[name]
}

func (j *Joiner) SchemaForName(name string) schema.Schema {
	return j.srcSchemas[name]
}
