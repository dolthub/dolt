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
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func addUniqIdxViols(
	ctx context.Context,
	postMergeSchema schema.Schema,
	index schema.Index,
	left, right, base prolly.Map,
	m prolly.Map,
	artEditor prolly.ArtifactsEditor,
	theirRootIsh hash.Hash,
	tblName string) error {

	meta, err := makeUniqViolMeta(postMergeSchema, index)
	if err != nil {
		return err
	}
	vInfo, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	kd := shim.KeyDescriptorFromSchema(index.Schema())
	prefixKD := kd.PrefixDesc(index.Count())
	prefixKB := val.NewTupleBuilder(prefixKD)
	p := left.Pool()

	suffixKD, _ := m.Descriptors()
	suffixKB := val.NewTupleBuilder(suffixKD)

	err = prolly.DiffMaps(ctx, base, right, func(ctx context.Context, diff tree.Diff) error {
		switch diff.Type {
		case tree.AddedDiff:
			pre := getPrefix(prefixKB, p, val.Tuple(diff.Key))
			itr, err := creation.NewPrefixItr(ctx, pre, prefixKD, left)
			if err != nil {
				return err
			}
			k, _, err := itr.Next(ctx)
			if err != nil && err != io.EOF {
				return nil
			}
			if err == nil {
				existingPK := getSuffix(suffixKB, p, k)
				newPK := getSuffix(suffixKB, p, val.Tuple(diff.Key))
				err = replaceUniqueKeyViolation(ctx, artEditor, m, existingPK, suffixKD, theirRootIsh, vInfo, tblName)
				if err != nil {
					return err
				}
				err = replaceUniqueKeyViolation(ctx, artEditor, m, newPK, suffixKD, theirRootIsh, vInfo, tblName)
				if err != nil {
					return err
				}
			}
		case tree.RemovedDiff:
		default:
			panic("unhandled diff type")
		}
		return nil
	})
	if err != io.EOF {
		return err
	}

	return nil
}

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

func (m UniqCVMeta) Unmarshall(ctx *sql.Context) (val sql.JSONDocument, err error) {
	return sql.JSONDocument{Val: m}, nil
}

func (m UniqCVMeta) Compare(ctx *sql.Context, v sql.JSONValue) (cmp int, err error) {
	ours := sql.JSONDocument{Val: m}
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

func replaceUniqueKeyViolation(ctx context.Context, edt prolly.ArtifactsEditor, m prolly.Map, k val.Tuple, kd val.TupleDesc, theirRootIsh hash.Hash, vInfo []byte, tblName string) error {
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

	err = edt.ReplaceConstraintViolation(ctx, k, theirRootIsh, prolly.ArtifactTypeUniqueKeyViol, meta)
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

func getPrefix(pKB *val.TupleBuilder, pool pool.BuffPool, k val.Tuple) val.Tuple {
	n := pKB.Desc.Count()
	for i := 0; i < n; i++ {
		pKB.PutRaw(i, k.GetField(i))
	}
	return pKB.Build(pool)
}

func getSuffix(sKB *val.TupleBuilder, pool pool.BuffPool, k val.Tuple) val.Tuple {
	n := sKB.Desc.Count()
	m := k.Count()
	for i, j := 0, m-n; j < k.Count(); i, j = i+1, j+1 {
		sKB.PutRaw(i, k.GetField(j))
	}
	return sKB.Build(pool)
}
