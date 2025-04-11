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
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func prollyParentSecDiffFkConstraintViolations(
	ctx context.Context,
	foreignKey doltdb.ForeignKey,
	postParent, postChild *constraintViolationsLoadedTable,
	preParentSecIdx prolly.Map,
	receiver FKViolationReceiver) error {
	postParentRowData, err := durable.ProllyMapFromIndex(postParent.RowData)
	if err != nil {
		return err
	}
	postParentSecIdx, err := durable.ProllyMapFromIndex(postParent.IndexData)
	if err != nil {
		return err
	}
	childSecIdx, err := durable.ProllyMapFromIndex(postChild.IndexData)
	if err != nil {
		return err
	}

	parentSecKD, _ := postParentSecIdx.Descriptors()
	parentPrefixKD := parentSecKD.PrefixDesc(len(foreignKey.TableColumns))
	partialKB := val.NewTupleBuilder(parentPrefixKD)

	childPriIdx, err := durable.ProllyMapFromIndex(postChild.RowData)
	if err != nil {
		return err
	}
	childPriKD, _ := childPriIdx.Descriptors()

	// TODO: Determine whether we should surface every row as a diff when the map's value descriptor has changed.
	considerAllRowsModified := false
	err = prolly.DiffMaps(ctx, preParentSecIdx, postParentSecIdx, considerAllRowsModified, func(ctx context.Context, diff tree.Diff) error {
		switch diff.Type {
		case tree.RemovedDiff, tree.ModifiedDiff:
			toSecKey, hadNulls := makePartialKey(partialKB, foreignKey.ReferencedTableColumns, postParent.Index, postParent.IndexSchema, val.Tuple(diff.Key), val.Tuple(diff.From), preParentSecIdx.Pool())
			if hadNulls {
				// row had some nulls previously, so it couldn't have been a parent
				return nil
			}

			ok, err := postParentSecIdx.HasPrefix(ctx, toSecKey, parentPrefixKD)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}

			// All equivalent parents were deleted, let's check for dangling children.
			// We search for matching keys in the child's secondary index
			err = createCVsForPartialKeyMatches(ctx, toSecKey, parentPrefixKD, childPriKD, childPriIdx, childSecIdx, postParentRowData.Pool(), receiver)
			if err != nil {
				return err
			}
		case tree.AddedDiff:
		default:
			panic("unhandled diff type")
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return err
	}

	return nil
}

func prollyParentPriDiffFkConstraintViolations(
	ctx context.Context,
	foreignKey doltdb.ForeignKey,
	postParent, postChild *constraintViolationsLoadedTable,
	preParentRowData prolly.Map,
	receiver FKViolationReceiver) error {
	postParentRowData, err := durable.ProllyMapFromIndex(postParent.RowData)
	if err != nil {
		return err
	}
	postParentIndexData, err := durable.ProllyMapFromIndex(postParent.IndexData)
	if err != nil {
		return err
	}

	idxDesc, _ := postParentIndexData.Descriptors()
	partialDesc := idxDesc.PrefixDesc(len(foreignKey.TableColumns))
	partialKB := val.NewTupleBuilder(partialDesc)

	childPriIdx, err := durable.ProllyMapFromIndex(postChild.RowData)
	if err != nil {
		return err
	}
	childScndryIdx, err := durable.ProllyMapFromIndex(postChild.IndexData)
	if err != nil {
		return err
	}
	primaryKD, _ := childPriIdx.Descriptors()

	// TODO: Determine whether we should surface every row as a diff when the map's value descriptor has changed.
	considerAllRowsModified := false
	err = prolly.DiffMaps(ctx, preParentRowData, postParentRowData, considerAllRowsModified, func(ctx context.Context, diff tree.Diff) error {
		switch diff.Type {
		case tree.RemovedDiff, tree.ModifiedDiff:
			partialKey, hadNulls := makePartialKey(partialKB, foreignKey.ReferencedTableColumns, postParent.Index, postParent.Schema, val.Tuple(diff.Key), val.Tuple(diff.From), preParentRowData.Pool())
			if hadNulls {
				// row had some nulls previously, so it couldn't have been a parent
				return nil
			}

			partialKeyRange := prolly.PrefixRange(ctx, partialKey, partialDesc)
			itr, err := postParentIndexData.IterRange(ctx, partialKeyRange)
			if err != nil {
				return err
			}

			_, _, err = itr.Next(ctx)
			if err != nil && err != io.EOF {
				return err
			}
			if err == nil {
				// some other equivalent parents exist
				return nil
			}

			// All equivalent parents were deleted, let's check for dangling children.
			// We search for matching keys in the child's secondary index
			err = createCVsForPartialKeyMatches(ctx, partialKey, partialDesc, primaryKD, childPriIdx, childScndryIdx, childPriIdx.Pool(), receiver)
			if err != nil {
				return err
			}

		case tree.AddedDiff:
		default:
			panic("unhandled diff type")
		}

		return nil
	})
	if err != nil && err != io.EOF {
		return err
	}

	return nil
}

func prollyChildPriDiffFkConstraintViolations(
	ctx context.Context,
	foreignKey doltdb.ForeignKey,
	postParent, postChild *constraintViolationsLoadedTable,
	preChildRowData prolly.Map,
	receiver FKViolationReceiver) error {
	postChildRowData, err := durable.ProllyMapFromIndex(postChild.RowData)
	if err != nil {
		return err
	}
	parentScndryIdx, err := durable.ProllyMapFromIndex(postParent.IndexData)
	if err != nil {
		return err
	}

	idxDesc, _ := parentScndryIdx.Descriptors()
	partialDesc := idxDesc.PrefixDesc(len(foreignKey.TableColumns))
	partialKB := val.NewTupleBuilder(partialDesc)

	// TODO: Determine whether we should surface every row as a diff when the map's value descriptor has changed.
	considerAllRowsModified := false
	err = prolly.DiffMaps(ctx, preChildRowData, postChildRowData, considerAllRowsModified, func(ctx context.Context, diff tree.Diff) error {
		switch diff.Type {
		case tree.AddedDiff, tree.ModifiedDiff:
			k, v := val.Tuple(diff.Key), val.Tuple(diff.To)
			partialKey, hasNulls := makePartialKey(
				partialKB,
				foreignKey.TableColumns,
				postChild.Index,
				postChild.Schema,
				k,
				v,
				preChildRowData.Pool())
			if hasNulls {
				return nil
			}

			err := createCVIfNoPartialKeyMatchesPri(ctx, k, v, partialKey, partialDesc, parentScndryIdx, receiver)
			if err != nil {
				return err
			}
		case tree.RemovedDiff:
		default:
			panic("unhandled diff type")
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return err
	}

	return nil
}

func prollyChildSecDiffFkConstraintViolations(
	ctx context.Context,
	foreignKey doltdb.ForeignKey,
	postParent, postChild *constraintViolationsLoadedTable,
	preChildSecIdx prolly.Map,
	receiver FKViolationReceiver) error {
	postChildRowData, err := durable.ProllyMapFromIndex(postChild.RowData)
	if err != nil {
		return err
	}
	postChildSecIdx, err := durable.ProllyMapFromIndex(postChild.IndexData)
	if err != nil {
		return err
	}
	parentSecIdx, err := durable.ProllyMapFromIndex(postParent.IndexData)
	if err != nil {
		return err
	}

	parentSecIdxDesc, _ := parentSecIdx.Descriptors()
	prefixDesc := parentSecIdxDesc.PrefixDesc(len(foreignKey.TableColumns))
	childPriKD, _ := postChildRowData.Descriptors()
	childPriKB := val.NewTupleBuilder(childPriKD)

	// TODO: Determine whether we should surface every row as a diff when the map's value descriptor has changed.
	considerAllRowsModified := false
	err = prolly.DiffMaps(ctx, preChildSecIdx, postChildSecIdx, considerAllRowsModified, func(ctx context.Context, diff tree.Diff) error {
		switch diff.Type {
		case tree.AddedDiff, tree.ModifiedDiff:
			k := val.Tuple(diff.Key)
			// TODO: possible to skip this if there are not null constraints over entire index
			for i := 0; i < k.Count(); i++ {
				if k.FieldIsNull(i) {
					return nil
				}
			}

			ok, err := parentSecIdx.HasPrefix(ctx, k, prefixDesc)
			if err != nil {
				return err
			} else if !ok {
				return createCVForSecIdx(ctx, k, childPriKD, childPriKB, postChildRowData, postChildRowData.Pool(), receiver)
			}
			return nil

		case tree.RemovedDiff:
		default:
			panic("unhandled diff type")
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

func createCVIfNoPartialKeyMatchesPri(
	ctx context.Context,
	k, v, partialKey val.Tuple,
	partialKeyDesc val.TupleDesc,
	idx prolly.Map,
	receiver FKViolationReceiver) error {
	itr, err := creation.NewPrefixItr(ctx, partialKey, partialKeyDesc, idx)
	if err != nil {
		return err
	}
	_, _, err = itr.Next(ctx)
	if err != nil && err != io.EOF {
		return err
	}
	if err == nil {
		return nil
	}

	return receiver.ProllyFKViolationFound(ctx, k, v)
}

func createCVForSecIdx(
	ctx context.Context,
	k val.Tuple,
	primaryKD val.TupleDesc,
	primaryKb *val.TupleBuilder,
	pri prolly.Map,
	pool pool.BuffPool,
	receiver FKViolationReceiver) error {

	// convert secondary idx entry to primary row key
	// the pks of the table are the last keys of the index
	o := k.Count() - primaryKD.Count()
	for i := 0; i < primaryKD.Count(); i++ {
		j := o + i
		primaryKb.PutRaw(i, k.GetField(j))
	}
	primaryIdxKey := primaryKb.Build(pool)

	var value val.Tuple
	err := pri.Get(ctx, primaryIdxKey, func(k, v val.Tuple) error {
		value = v
		return nil
	})
	if err != nil {
		return err
	}

	return receiver.ProllyFKViolationFound(ctx, primaryIdxKey, value)
}

func createCVsForPartialKeyMatches(
	ctx context.Context,
	partialKey val.Tuple,
	partialKeyDesc val.TupleDesc,
	primaryKD val.TupleDesc,
	primaryIdx prolly.Map,
	secondaryIdx prolly.Map,
	pool pool.BuffPool,
	receiver FKViolationReceiver,
) error {

	itr, err := creation.NewPrefixItr(ctx, partialKey, partialKeyDesc, secondaryIdx)
	if err != nil {
		return err
	}

	kb := val.NewTupleBuilder(primaryKD)

	for k, _, err := itr.Next(ctx); err == nil; k, _, err = itr.Next(ctx) {

		// convert secondary idx entry to primary row key
		// the pks of the table are the last keys of the index
		o := k.Count() - primaryKD.Count()
		for i := 0; i < primaryKD.Count(); i++ {
			j := o + i
			kb.PutRaw(i, k.GetField(j))
		}
		primaryIdxKey := kb.Build(pool)

		var value val.Tuple
		err := primaryIdx.Get(ctx, primaryIdxKey, func(k, v val.Tuple) error {
			value = v
			return nil
		})
		if err != nil {
			return err
		}

		err = receiver.ProllyFKViolationFound(ctx, primaryIdxKey, value)
		if err != nil {
			return err
		}
	}
	if err != nil && err != io.EOF {
		return err
	}

	return nil
}

func makePartialKey(kb *val.TupleBuilder, tags []uint64, idxSch schema.Index, tblSch schema.Schema, k, v val.Tuple, pool pool.BuffPool) (val.Tuple, bool) {
	// Possible that the parent index (idxSch) is longer than the partial key (tags).
	if idxSch.Name() != "" && len(idxSch.IndexedColumnTags()) <= len(tags) {
		tags = idxSch.IndexedColumnTags()
	}
	for i, tag := range tags {
		if j, ok := tblSch.GetPKCols().TagToIdx[tag]; ok {
			if k.FieldIsNull(j) {
				return nil, true
			}
			kb.PutRaw(i, k.GetField(j))
			continue
		}

		j, _ := tblSch.GetNonPKCols().TagToIdx[tag]
		if v.FieldIsNull(j) {
			return nil, true
		}
		if schema.IsKeyless(tblSch) {
			kb.PutRaw(i, v.GetField(j+1))
		} else {
			kb.PutRaw(i, v.GetField(j))
		}
	}

	return kb.Build(pool), false
}

// TODO: Change json.NomsJson string marshalling to match json.Marshall
// Currently it returns additional whitespace. Another option is to implement a
// custom json encoder that matches json.NomsJson string marshalling.

type FkCVMeta struct {
	Columns           []string `json:"Columns"`
	ForeignKey        string   `json:"ForeignKey"`
	Index             string   `json:"Index"`
	OnDelete          string   `json:"OnDelete"`
	OnUpdate          string   `json:"OnUpdate"`
	ReferencedColumns []string `json:"ReferencedColumns"`
	ReferencedIndex   string   `json:"ReferencedIndex"`
	ReferencedTable   string   `json:"ReferencedTable"`
	Table             string   `json:"Table"`
}

func (m FkCVMeta) Clone(_ context.Context) sql.JSONWrapper {
	return m
}

var _ sql.JSONWrapper = FkCVMeta{}

func (m FkCVMeta) ToInterface() (interface{}, error) {
	return map[string]interface{}{
		"Columns":           m.Columns,
		"ForeignKey":        m.ForeignKey,
		"Index":             m.Index,
		"OnDelete":          m.OnDelete,
		"OnUpdate":          m.OnUpdate,
		"ReferencedColumns": m.ReferencedColumns,
		"ReferencedIndex":   m.ReferencedIndex,
		"ReferencedTable":   m.ReferencedTable,
		"Table":             m.Table,
	}, nil
}

// PrettyPrint is a custom pretty print function to match the old format's
// output which includes additional whitespace between keys, values, and array elements.
func (m FkCVMeta) PrettyPrint() string {
	jsonStr := fmt.Sprintf(`{`+
		`"Index": "%s", `+
		`"Table": "%s", `+
		`"Columns": ["%s"], `+
		`"OnDelete": "%s", `+
		`"OnUpdate": "%s", `+
		`"ForeignKey": "%s", `+
		`"ReferencedIndex": "%s", `+
		`"ReferencedTable": "%s", `+
		`"ReferencedColumns": ["%s"]}`,
		m.Index,
		m.Table,
		strings.Join(m.Columns, `', '`),
		m.OnDelete,
		m.OnUpdate,
		m.ForeignKey,
		m.ReferencedIndex,
		m.ReferencedTable,
		strings.Join(m.ReferencedColumns, `', '`))
	return jsonStr
}
