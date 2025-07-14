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

func (m UniqCVMeta) Clone(_ context.Context) sql.JSONWrapper {
	return m
}

func (m UniqCVMeta) ToInterface(context.Context) (interface{}, error) {
	return map[string]interface{}{
		"Columns": m.Columns,
		"Name":    m.Name,
	}, nil
}

var _ sql.JSONWrapper = UniqCVMeta{}

func (m UniqCVMeta) Unmarshall(ctx *sql.Context) (val types.JSONDocument, err error) {
	return types.JSONDocument{Val: m}, nil
}

func (m UniqCVMeta) PrettyPrint() string {
	jsonStr := fmt.Sprintf(`{`+
		`"Name": "%s", `+
		`"Columns": ["%s"]}`,
		m.Name,
		strings.Join(m.Columns, `', '`))
	return jsonStr
}

func replaceUniqueKeyViolation(ctx context.Context, edt *prolly.ArtifactsEditor, m prolly.Map, k val.Tuple, theirRootIsh doltdb.Rootish, vInfo []byte) error {
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
		return err
	}

	return nil
}

func getPKFromSecondaryKey(pKB *val.TupleBuilder, pool pool.BuffPool, pkMapping val.OrdinalMapping, k val.Tuple) (val.Tuple, error) {
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

type NullViolationMeta struct {
	Columns []string `json:"Columns"`
}

func (m NullViolationMeta) Clone(_ context.Context) sql.JSONWrapper {
	return m
}

var _ sql.JSONWrapper = NullViolationMeta{}

func newNotNullViolationMeta(violations []string, value val.Tuple) (prolly.ConstraintViolationMeta, error) {
	info, err := json.Marshal(NullViolationMeta{Columns: violations})
	if err != nil {
		return prolly.ConstraintViolationMeta{}, err
	}
	return prolly.ConstraintViolationMeta{
		VInfo: info,
		Value: value,
	}, nil
}

func (m NullViolationMeta) ToInterface(context.Context) (interface{}, error) {
	return map[string]interface{}{
		"Columns": m.Columns,
	}, nil
}

func (m NullViolationMeta) Unmarshall(ctx *sql.Context) (val types.JSONDocument, err error) {
	return types.JSONDocument{Val: m}, nil
}

// CheckCVMeta holds metadata describing a check constraint violation.
type CheckCVMeta struct {
	Name       string `json:"Name"`
	Expression string `json:"Expression"`
}

func (m CheckCVMeta) Clone(_ context.Context) sql.JSONWrapper {
	return m
}

var _ sql.JSONWrapper = CheckCVMeta{}

// newCheckCVMeta creates a new CheckCVMeta from a schema |sch| and a check constraint name |checkName|. If the
// check constraint is not found in the specified schema, an error is returned.
func newCheckCVMeta(sch schema.Schema, checkName string) (CheckCVMeta, error) {
	found := false
	var check schema.Check
	for _, check = range sch.Checks().AllChecks() {
		if check.Name() == checkName {
			found = true
			break
		}
	}
	if !found {
		return CheckCVMeta{}, fmt.Errorf("check constraint '%s' not found in schema", checkName)
	}

	return CheckCVMeta{
		Name:       check.Name(),
		Expression: check.Expression(),
	}, nil
}

// Unmarshall implements sql.JSONWrapper
func (m CheckCVMeta) Unmarshall(_ *sql.Context) (val types.JSONDocument, err error) {
	return types.JSONDocument{Val: m}, nil
}

func (m CheckCVMeta) ToInterface(context.Context) (interface{}, error) {
	return map[string]interface{}{
		"Name":       m.Name,
		"Expression": m.Expression,
	}, nil
}
