// Copyright 2022 Dolthub, Inc.
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

package merge

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

func makeUniqViolMeta(sch schema.Schema, idx schema.Index) (UniqCVMeta, error) {
	schCols := sch.GetAllCols()
	idxTags := idx.IndexedColumnTags()
	colNames := make([]string, len(idxTags))
	for i, tag := range idxTags {
		if col, ok := schCols.TagToCol[tag]; !ok {
			return UniqCVMeta{}, fmt.Errorf("unique key '%s' references tag '%d' on table but it cannot be found",
				idx.Name(), tag)
		} else {
			colNames[i] = col.Name
		}
	}

	return UniqCVMeta{
		Columns: colNames,
		Name:    idx.Name(),
	}, nil
}

type UniqCVMeta struct {
	Columns []string `json:"Columns"`
	Name    string   `json:"Name"`
}

func (m UniqCVMeta) Unmarshall(ctx *sql.Context) (val types.JSONDocument, err error) {
	return types.JSONDocument{Val: m}, nil
}

func (m UniqCVMeta) Compare(ctx *sql.Context, v types.JSONValue) (cmp int, err error) {
	ours := types.JSONDocument{Val: m}
	return ours.Compare(ctx, v)
}

func (m UniqCVMeta) ToString(ctx *sql.Context) (string, error) {
	return m.PrettyPrint(), nil
}

func (m UniqCVMeta) PrettyPrint() string {
	jsonStr := fmt.Sprintf(`{`+
		`"Columns": ["%s"], `+
		`"Name": "%s"}`,
		strings.Join(m.Columns, `', '`),
		m.Name)
	return jsonStr
}

func replaceUniqueKeyViolation(ctx context.Context, edt *prolly.ArtifactsEditor, m prolly.Map, k val.Tuple, kd val.TupleDesc, theirRootIsh doltdb.Rootish, vInfo []byte, tblName string) error {
	var value val.Tuple
	err := m.Get(ctx, k, func(_, v val.Tuple) error {
		value = v
		return nil
	})
	if err != nil {
		return err
	}

	meta := prolly.ConstraintViolationMeta{
		VInfo: vInfo,
		Value: value,
	}

	theirsHash, err := theirRootIsh.HashOf()
	if err != nil {
		return err
	}

	err = edt.ReplaceConstraintViolation(ctx, k, theirsHash, prolly.ArtifactTypeUniqueKeyViol, meta)
	if err != nil {
		if mv, ok := err.(*prolly.ErrMergeArtifactCollision); ok {
			var e, n UniqCVMeta
			err = json.Unmarshal(mv.ExistingInfo, &e)
			if err != nil {
				return err
			}
			err = json.Unmarshal(mv.NewInfo, &n)
			if err != nil {
				return err
			}
			return fmt.Errorf("%w: pk %s of table '%s' violates unique keys '%s' and '%s'",
				ErrMultipleViolationsForRow,
				kd.Format(mv.Key), tblName, e.Name, n.Name)
		}
		return err
	}

	return nil
}

func replaceUniqueKeyViolationWithValue(ctx context.Context, edt *prolly.ArtifactsEditor, k, value val.Tuple, kd val.TupleDesc, theirRootIsh doltdb.Rootish, vInfo []byte, tblName string) error {
	meta := prolly.ConstraintViolationMeta{
		VInfo: vInfo,
		Value: value,
	}

	theirsHash, err := theirRootIsh.HashOf()
	if err != nil {
		return err
	}

	err = edt.ReplaceConstraintViolation(ctx, k, theirsHash, prolly.ArtifactTypeUniqueKeyViol, meta)
	if err != nil {
		if mv, ok := err.(*prolly.ErrMergeArtifactCollision); ok {
			var e, n UniqCVMeta
			err = json.Unmarshal(mv.ExistingInfo, &e)
			if err != nil {
				return err
			}
			err = json.Unmarshal(mv.NewInfo, &n)
			if err != nil {
				return err
			}
			return fmt.Errorf("%w: pk %s of table '%s' violates unique keys '%s' and '%s'",
				ErrMultipleViolationsForRow,
				kd.Format(mv.Key), tblName, e.Name, n.Name)
		}
		return err
	}

	return nil
}

func getPKFromSecondaryKey(pKB *val.TupleBuilder, pool pool.BuffPool, pkMapping val.OrdinalMapping, k val.Tuple) val.Tuple {
	for to := range pkMapping {
		from := pkMapping.MapOrdinal(to)
		pKB.PutRaw(to, k.GetField(from))
	}
	return pKB.Build(pool)
}

func ordinalMappingFromIndex(def schema.Index) (m val.OrdinalMapping) {
	pks := def.PrimaryKeyTags()
	if len(pks) == 0 { // keyless index
		m = make(val.OrdinalMapping, 1)
		m[0] = len(def.AllTags())
		return m
	}

	m = make(val.OrdinalMapping, len(pks))
	for i, pk := range pks {
		for j, tag := range def.AllTags() {
			if tag == pk {
				m[i] = j
				break
			}
		}
	}
	return
}
