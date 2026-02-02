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
	"github.com/sirupsen/logrus"

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

	parentSecIdxDesc, _ := postParentSecIdx.Descriptors()
	parentIdxPrefixDesc := parentSecIdxDesc.PrefixDesc(len(foreignKey.TableColumns))
	parentIdxKb := val.NewTupleBuilder(parentIdxPrefixDesc, postParentRowData.NodeStore())

	childPriIdx, err := durable.ProllyMapFromIndex(postChild.RowData)
	if err != nil {
		return err
	}

	childPriIdxDesc, _ := childPriIdx.Descriptors()
	childSecIdxDesc, _ := childSecIdx.Descriptors()

	childPrimary := &indexAndKeyDescriptor{
		index:   childPriIdx,
		keyDesc: childPriIdxDesc,
	}
	childSecondary := &indexAndKeyDescriptor{
		index:   childSecIdx,
		keyDesc: childSecIdxDesc,
	}

	// We allow foreign keys between types that don't have the same serialization bytes for the same logical values
	// in some contexts. If this lookup is one of those, we need to convert the child key to the parent key format.
	compatibleTypes := foreignKeySerializationCompatible(parentIdxPrefixDesc, childSecIdxDesc)

	// TODO: Determine whether we should surface every row as a diff when the map's value descriptor has changed.
	considerAllRowsModified := false
	err = prolly.DiffMaps(ctx, preParentSecIdx, postParentSecIdx, considerAllRowsModified, func(ctx context.Context, diff tree.Diff) error {
		switch diff.Type {
		case tree.RemovedDiff, tree.ModifiedDiff:
			k, hadNulls, err := makePartialKey(parentIdxKb, foreignKey.ReferencedTableColumns, postParent.Index, postParent.IndexSchema, val.Tuple(diff.Key), val.Tuple(diff.From), preParentSecIdx.Pool())
			if err != nil {
				return err
			}
			if hadNulls {
				// row had some nulls previously, so it couldn't have been a parent
				return nil
			}

			ok, err := postParentSecIdx.HasPrefix(ctx, k, parentIdxPrefixDesc)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}

			// All equivalent parents were deleted, let's check for dangling children.
			// We search for matching keys in the child's secondary index
			err = createCVsForDanglingChildRows(
				ctx,
				k,
				parentIdxPrefixDesc,
				childPrimary,
				childSecondary,
				receiver,
				compatibleTypes,
			)
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

// foreignKeySerializationCompatible returns whether the serializations for two tuple descriptors are
// binary compatible, which is requirement when using them as values in during foreign key lookups.
func foreignKeySerializationCompatible(descA, descB *val.TupleDesc) bool {
	compatibleTypes := true
	for i, handler := range descA.Handlers {
		if handler != nil && !handler.SerializationCompatible(descB.Handlers[i]) {
			compatibleTypes = false
			break
		}
	}
	return compatibleTypes
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
	partialKB := val.NewTupleBuilder(partialDesc, postParentRowData.NodeStore())

	childPriIdx, err := durable.ProllyMapFromIndex(postChild.RowData)
	if err != nil {
		return err
	}
	childScndryIdx, err := durable.ProllyMapFromIndex(postChild.IndexData)
	if err != nil {
		return err
	}

	childPriIdxDesc, _ := childPriIdx.Descriptors()
	childSecIdxDesc, _ := childScndryIdx.Descriptors()

	childPrimary := &indexAndKeyDescriptor{
		index:   childPriIdx,
		keyDesc: childPriIdxDesc,
	}
	childSecondary := &indexAndKeyDescriptor{
		index:   childScndryIdx,
		keyDesc: childSecIdxDesc,
	}

	// We allow foreign keys between types that don't have the same serialization bytes for the same logical values
	// in some contexts. If this lookup is one of those, we need to convert the child key to the parent key format.
	compatibleTypes := foreignKeySerializationCompatible(partialDesc, childSecIdxDesc)

	// TODO: Determine whether we should surface every row as a diff when the map's value descriptor has changed.
	considerAllRowsModified := false
	err = prolly.DiffMaps(ctx, preParentRowData, postParentRowData, considerAllRowsModified, func(ctx context.Context, diff tree.Diff) error {
		switch diff.Type {
		case tree.RemovedDiff, tree.ModifiedDiff:
			partialKey, hadNulls, err := makePartialKey(partialKB, foreignKey.ReferencedTableColumns, postParent.Index, postParent.Schema, val.Tuple(diff.Key), val.Tuple(diff.From), preParentRowData.Pool())
			if err != nil {
				return err
			}
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
			err = createCVsForDanglingChildRows(
				ctx,
				partialKey,
				partialDesc,
				childPrimary,
				childSecondary,
				receiver,
				compatibleTypes,
			)
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
	partialKB := val.NewTupleBuilder(partialDesc, postChildRowData.NodeStore())

	// TODO: Determine whether we should surface every row as a diff when the map's value descriptor has changed.
	considerAllRowsModified := false
	err = prolly.DiffMaps(ctx, preChildRowData, postChildRowData, considerAllRowsModified, func(ctx context.Context, diff tree.Diff) error {
		switch diff.Type {
		case tree.AddedDiff, tree.ModifiedDiff:
			k, v := val.Tuple(diff.Key), val.Tuple(diff.To)
			partialKey, hasNulls, err := makePartialKey(
				partialKB,
				foreignKey.TableColumns,
				postChild.Index,
				postChild.Schema,
				k,
				v,
				preChildRowData.Pool())
			if err != nil {
				return err
			}
			if hasNulls {
				return nil
			}

			err = createCVIfNoPartialKeyMatchesPri(ctx, k, v, partialKey, partialDesc, parentScndryIdx, receiver)
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
	parentIdxPrefixDesc := parentSecIdxDesc.PrefixDesc(len(foreignKey.TableColumns))
	childPriKD, _ := postChildRowData.Descriptors()
	childIdxDesc, _ := postChildSecIdx.Descriptors()
	childIdxPrefixDesc := childIdxDesc.PrefixDesc(len(foreignKey.TableColumns))
	childPriKB := val.NewTupleBuilder(childPriKD, preChildSecIdx.NodeStore())

	// We allow foreign keys between types that don't have the same serialization bytes for the same logical values
	// in some contexts. If this lookup is one of those, we need to convert the child key to the parent key format.
	compatibleTypes := true
	for i, handler := range childIdxPrefixDesc.Handlers {
		if handler != nil && !handler.SerializationCompatible(parentIdxPrefixDesc.Handlers[i]) {
			compatibleTypes = false
			break
		}
	}

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

			if !compatibleTypes {
				tb := val.NewTupleBuilder(parentIdxPrefixDesc, postChildSecIdx.NodeStore())
				for i, childHandler := range childIdxPrefixDesc.Handlers {
					parentHandler := parentIdxPrefixDesc.Handlers[i]
					serialized, err := convertSerializedFkField(ctx, parentHandler, childHandler, k.GetField(i))
					if err != nil {
						return err
					}

					switch parentHandler.(type) {
					case val.AdaptiveEncodingTypeHandler:
						switch parentIdxPrefixDesc.Types[i].Enc {
						case val.ExtendedAdaptiveEnc:
							err := tb.PutAdaptiveExtendedFromInline(ctx, i, serialized)
							if err != nil {
								return err
							}
						case val.BytesAdaptiveEnc:
							err := tb.PutAdaptiveExtendedFromInline(ctx, i, serialized)
							if err != nil {
								return err
							}
						default:
							panic(fmt.Sprintf("unexpected encoding for adaptive type: %d", parentIdxPrefixDesc.Types[i].Enc))
						}
					default:
						tb.PutRaw(i, serialized)
					}
				}

				k, err = tb.Build(parentSecIdx.Pool())
				if err != nil {
					return err
				}
			}

			logrus.Warnf("looking for parent sec idx key: %s\n", parentSecIdxDesc.Format(ctx, k))
			logrus.Warnf("prefix key: %s\n", parentIdxPrefixDesc.Format(ctx, k))
			fullIdx, err := prolly.DebugFormat(ctx, parentSecIdx)
			if err != nil {
				return err
			}

			logrus.Warnf("sec idx: %s\n", fullIdx)

			ok, err := parentSecIdx.HasPrefix(ctx, k, parentIdxPrefixDesc)
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

// convertSerializedFkField converts a serialized foreign key value from one type handler to another.
func convertSerializedFkField(
	ctx context.Context,
	toHandler, fromHandler val.TupleTypeHandler,
	field []byte,
) ([]byte, error) {
	convertingHandler := toHandler
	convertedHandler := fromHandler

	switch h := toHandler.(type) {
	case val.AdaptiveEncodingTypeHandler:
		convertingHandler = h.ChildHandler()
	case val.AddressTypeHandler:
		convertingHandler = h.ChildHandler()
	}

	switch h := fromHandler.(type) {
	case val.AdaptiveEncodingTypeHandler:
		convertedHandler = h.ChildHandler()
		adaptiveVal := val.AdaptiveValue(field)
		if adaptiveVal.IsOutOfBand() {
			var err error
			field, err = h.ConvertToInline(ctx, adaptiveVal)
			if err != nil {
				return nil, err
			}
		}
		field = field[1:]
	case val.AddressTypeHandler:
		convertedHandler = h.ChildHandler()
		unhashed, err := h.DeserializeValue(ctx, field)
		if err != nil {
			return nil, err
		}

		serialized, err := h.ChildHandler().SerializeValue(ctx, unhashed)
		if err != nil {
			return nil, err
		}

		field = serialized
	}

	serialized, err := convertingHandler.ConvertSerialized(ctx, convertedHandler, field)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

func createCVIfNoPartialKeyMatchesPri(
	ctx context.Context,
	k, v, partialKey val.Tuple,
	partialKeyDesc *val.TupleDesc,
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
	primaryKD *val.TupleDesc,
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
	primaryIdxKey, err := primaryKb.Build(pool)
	if err != nil {
		return err
	}

	var value val.Tuple
	err = pri.Get(ctx, primaryIdxKey, func(k, v val.Tuple) error {
		value = v
		return nil
	})
	if err != nil {
		return err
	}
	if value == nil {
		return fmt.Errorf("unable to find row from secondary index in the primary index with key: %v", primaryKD.Format(ctx, primaryIdxKey))
	}

	return receiver.ProllyFKViolationFound(ctx, primaryIdxKey, value)
}

type indexAndKeyDescriptor struct {
	index   prolly.Map
	keyDesc *val.TupleDesc
}

// createCVsForDanglingChildRows finds all rows in the childIdx that match the given parent key and creates constraint
// violations for each of them using the provided receiver.
func createCVsForDanglingChildRows(
	ctx context.Context,
	partialKey val.Tuple,
	partialKeyDesc *val.TupleDesc,
	childPrimaryIdx *indexAndKeyDescriptor,
	childSecIdx *indexAndKeyDescriptor,
	receiver FKViolationReceiver,
	compatibleTypes bool,
) error {

	// We allow foreign keys between types that don't have the same serialization bytes for the same logical values
	// in some contexts. If this lookup is one of those, we need to convert the parent key to the child key format.
	if !compatibleTypes {
		tb := val.NewTupleBuilder(childSecIdx.keyDesc, childSecIdx.index.NodeStore())
		for i, parentHandler := range partialKeyDesc.Handlers {
			childHandler := childSecIdx.keyDesc.Handlers[i]
			serialized, err := convertSerializedFkField(ctx, childHandler, parentHandler, partialKey.GetField(i))
			if err != nil {
				return err
			}

			switch childHandler.(type) {
			case val.AdaptiveEncodingTypeHandler:
				switch partialKeyDesc.Types[i].Enc {
				case val.ExtendedAdaptiveEnc:
					err := tb.PutAdaptiveExtendedFromInline(ctx, i, serialized)
					if err != nil {
						return err
					}
				case val.BytesAdaptiveEnc:
					err := tb.PutAdaptiveExtendedFromInline(ctx, i, serialized)
					if err != nil {
						return err
					}
				default:
					panic(fmt.Sprintf("unexpected encoding for adaptive type: %d", partialKeyDesc.Types[i].Enc))
				}
			default:
				tb.PutRaw(i, serialized)
			}
		}

		var err error
		partialKey, err = tb.Build(childPrimaryIdx.index.Pool())
		if err != nil {
			return err
		}
	}

	itr, err := creation.NewPrefixItr(ctx, partialKey, partialKeyDesc, childSecIdx.index)
	if err != nil {
		return err
	}

	kb := val.NewTupleBuilder(childPrimaryIdx.keyDesc, childPrimaryIdx.index.NodeStore())

	for k, _, err := itr.Next(ctx); err != io.EOF; k, _, err = itr.Next(ctx) {
		if err != nil {
			return err
		}

		// convert secondary idx entry to primary row key
		// the pks of the table are the last keys of the index
		o := k.Count() - childPrimaryIdx.keyDesc.Count()
		for i := 0; i < childPrimaryIdx.keyDesc.Count(); i++ {
			j := o + i
			kb.PutRaw(i, k.GetField(j))
		}
		primaryIdxKey, err := kb.Build(childPrimaryIdx.index.Pool())
		if err != nil {
			return err
		}

		var value val.Tuple
		err = childPrimaryIdx.index.Get(ctx, primaryIdxKey, func(k, v val.Tuple) error {
			value = v
			return nil
		})
		if err != nil {
			return err
		}

		// If a value wasn't found, then there is a row in the secondary index
		// that can't be found in the primary index. This is never expected, so
		// we return an error.
		if value == nil {
			return fmt.Errorf("unable to find row from secondary index in the primary index, with key: %v", primaryIdxKey)
		}

		// If a value was found, then there is a row in the child table that references the
		// deleted parent row, so we report a constraint violation.
		err = receiver.ProllyFKViolationFound(ctx, primaryIdxKey, value)
		if err != nil {
			return err
		}
	}

	return nil
}

func makePartialKey(kb *val.TupleBuilder, tags []uint64, idxSch schema.Index, tblSch schema.Schema, k, v val.Tuple, pool pool.BuffPool) (val.Tuple, bool, error) {
	// Possible that the parent index (idxSch) is longer than the partial key (tags).
	if idxSch.Name() != "" && len(idxSch.IndexedColumnTags()) <= len(tags) {
		tags = idxSch.IndexedColumnTags()
	}
	for i, tag := range tags {
		if j, ok := tblSch.GetPKCols().TagToIdx[tag]; ok {
			if k.FieldIsNull(j) {
				return nil, true, nil
			}
			kb.PutRaw(i, k.GetField(j))
			continue
		}

		j, _ := tblSch.GetNonPKCols().TagToIdx[tag]
		if v.FieldIsNull(j) {
			return nil, true, nil
		}
		if schema.IsKeyless(tblSch) {
			kb.PutRaw(i, v.GetField(j+1))
		} else {
			kb.PutRaw(i, v.GetField(j))
		}
	}

	tup, err := kb.Build(pool)
	return tup, false, err
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

func (m FkCVMeta) ToInterface(context.Context) (interface{}, error) {
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
