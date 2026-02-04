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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
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
		schema:  postChild.Schema,
	}
	childSecondary := &indexAndKeyDescriptor{
		index:   childSecIdx,
		keyDesc: childSecIdxDesc,
		schema:  postChild.IndexSchema,
	}

	// We allow foreign keys between types that don't have the same serialization bytes for the same logical values
	// in some contexts. If this lookup is one of those, we need to convert the child key to the parent key format.
	compatibleTypes := foreignKeysAreCompatibleTypes(parentIdxPrefixDesc, childSecIdxDesc)

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
		schema:  postChild.Schema,
	}
	childSecondary := &indexAndKeyDescriptor{
		index:   childScndryIdx,
		keyDesc: childSecIdxDesc,
		schema:  postChild.IndexSchema,
	}

	// We allow foreign keys between types that don't have the same serialization bytes for the same logical values
	// in some contexts. If this lookup is one of those, we need to convert the child key to the parent key format.
	compatibleTypes := foreignKeysAreCompatibleTypes(partialDesc, childSecIdxDesc)

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
	parentSecondaryIdx, err := durable.ProllyMapFromIndex(postParent.IndexData)
	if err != nil {
		return err
	}

	childPriIdxDesc, _ := postChildRowData.Descriptors()
	parentIdxDesc, _ := parentSecondaryIdx.Descriptors()
	parentIdxPrefixDesc := parentIdxDesc.PrefixDesc(len(foreignKey.TableColumns))
	partialKB := val.NewTupleBuilder(parentIdxPrefixDesc, postChildRowData.NodeStore())

	// We allow foreign keys between types that don't have the same serialization bytes for the same logical values
	// in some contexts. If this lookup is one of those, we need to convert the child key to the parent key format.
	compatibleTypes := foreignKeysAreCompatibleTypes(childPriIdxDesc, parentIdxPrefixDesc)

	// TODO: Determine whether we should surface every row as a diff when the map's value descriptor has changed.
	considerAllRowsModified := false
	err = prolly.DiffMaps(ctx, preChildRowData, postChildRowData, considerAllRowsModified, func(ctx context.Context, diff tree.Diff) error {
		switch diff.Type {
		case tree.AddedDiff, tree.ModifiedDiff:
			k, v := val.Tuple(diff.Key), val.Tuple(diff.To)
			parentLookupKey, hasNulls, err := makePartialKey(
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

			if !compatibleTypes {
				parentLookupKey, err = convertKeyBetweenTypes(ctx, parentLookupKey, childPriIdxDesc, parentIdxPrefixDesc, parentSecondaryIdx.NodeStore(), parentSecondaryIdx.Pool())
				if err != nil {
					return err
				}
			}

			err = createCVIfNoPartialKeyMatchesPri(ctx, k, v, parentLookupKey, parentIdxPrefixDesc, parentSecondaryIdx, receiver)
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

	// We allow foreign keys between types that don't have the same serialization bytes for the same logical values
	// in some contexts. If this lookup is one of those, we need to convert the child key to the parent key format.
	compatibleTypes := foreignKeysAreCompatibleTypes(childIdxPrefixDesc, parentIdxPrefixDesc)

	// TODO: Determine whether we should surface every row as a diff when the map's value descriptor has changed.
	considerAllRowsModified := false
	err = prolly.DiffMaps(ctx, preChildSecIdx, postChildSecIdx, considerAllRowsModified, func(ctx context.Context, diff tree.Diff) error {
		switch diff.Type {
		case tree.AddedDiff, tree.ModifiedDiff:
			key := val.Tuple(diff.Key)
			parentLookupKey := key

			// TODO: possible to skip this if there are not null constraints over entire index
			for i := 0; i < parentLookupKey.Count(); i++ {
				if parentLookupKey.FieldIsNull(i) {
					return nil
				}
			}

			if !compatibleTypes {
				parentLookupKey, err = convertKeyBetweenTypes(ctx, key, childIdxPrefixDesc, parentIdxPrefixDesc, postChildSecIdx.NodeStore(), postChildSecIdx.Pool())
				if err != nil {
					return err
				}
			}

			ok, err := parentSecIdx.HasPrefix(ctx, parentLookupKey, parentIdxPrefixDesc)
			if err != nil {
				return err
			} else if !ok {
				return createCVForSecIdx(ctx, key, childPriKD, postChildRowData, postChild.Schema, postChild.IndexSchema, receiver)
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

// foreignKeysAreCompatibleTypes returns whether the serializations for two tuple descriptors are binary compatible
func foreignKeysAreCompatibleTypes(keyDescA, keyDescB *val.TupleDesc) bool {
	compatibleTypes := true
	for i, handlerA := range keyDescA.Handlers {
		handlerB := keyDescB.Handlers[i]
		if handlerA != nil && handlerB != nil && !handlerA.SerializationCompatible(handlerB) {
			compatibleTypes = false
			break
		}
	}
	return compatibleTypes
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
		pri prolly.Map,
		tableSchema, indexSchema schema.Schema,
		receiver FKViolationReceiver,
) error {

	// convert secondary idx entry to primary row key
	primaryKey, err := primaryKeyFromSecondaryIndexRow(k, primaryKD, pri, tableSchema, indexSchema)
	if err != nil {
		return err
	}

	var value val.Tuple
	err = pri.Get(ctx, primaryKey, func(k, v val.Tuple) error {
		value = v
		return nil
	})
	if err != nil {
		return err
	}
	if value == nil {
		return fmt.Errorf("unable to find row from secondary index in the primary index with key: %v", primaryKD.Format(ctx, primaryKey))
	}

	return receiver.ProllyFKViolationFound(ctx, primaryKey, value)
}

func primaryKeyFromSecondaryIndexRow(secIndexRow val.Tuple, primaryKD *val.TupleDesc, pri prolly.Map, tableSchema schema.Schema, indexSchema schema.Schema) (val.Tuple, error) {
	keyMap := makeOrdinalMappingForSchemas(indexSchema, tableSchema)

	kb := val.NewTupleBuilder(primaryKD, pri.NodeStore())
	for to := range keyMap {
		from := keyMap.MapOrdinal(to)
		kb.PutRaw(to, secIndexRow.GetField(from))
	}

	return kb.Build(pri.Pool())
}

// makeOrdinalMappingForSchemas creates an ordinal mapping from one schema to another based on column names.
func makeOrdinalMappingForSchemas(fromSch, toSch schema.Schema) (m val.OrdinalMapping) {
	from := fromSch.GetPKCols()
	to := toSch.GetPKCols()

	// offset accounts for a keyless to schema, where the pseudo key column is the final one
	offset := 0
	if from.Size() == 0 {
		from = fromSch.GetNonPKCols()
	}
	if to.Size() == 0 {
		to = toSch.GetNonPKCols()
		offset = to.Size()
	}

	m = make(val.OrdinalMapping, to.StoredSize())
	for i := range m {
		col := to.GetByStoredIndex(i)
		name := col.Name
		colIdx := from.IndexOf(name)
		m[i] = colIdx + offset
	}
	return
}

type indexAndKeyDescriptor struct {
	index   prolly.Map
	keyDesc *val.TupleDesc
	schema  schema.Schema
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
	secondaryIndexKeyDesc := childSecIdx.keyDesc.PrefixDesc(partialKeyDesc.Count())
	if !compatibleTypes {
		var err error
		partialKey, err = convertKeyBetweenTypes(ctx, partialKey, partialKeyDesc, secondaryIndexKeyDesc, childSecIdx.index.NodeStore(), childPrimaryIdx.index.Pool())
		if err != nil {
			return err
		}
	}

	itr, err := creation.NewPrefixItr(ctx, partialKey, secondaryIndexKeyDesc, childSecIdx.index)
	if err != nil {
		return err
	}

	for k, _, err := itr.Next(ctx); err != io.EOF; k, _, err = itr.Next(ctx) {
		if err != nil {
			return err
		}

		// convert secondary idx entry to primary row key
		primaryIdxKey, err := primaryKeyFromSecondaryIndexRow(k, childPrimaryIdx.keyDesc, childPrimaryIdx.index, childPrimaryIdx.schema, childSecIdx.schema)
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
			return fmt.Errorf("unable to find row from secondary index in the primary index, with key: %v", childPrimaryIdx.keyDesc.Format(ctx, primaryIdxKey))
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

// convertKeyBetweenTypes converts a partial key from one tuple descriptor's types to another. This is only necessary
// when the keys are of different types that are not serialization identical.
func convertKeyBetweenTypes(
		ctx context.Context,
		key val.Tuple,
		fromKeyDesc *val.TupleDesc,
		toKeyDesc *val.TupleDesc,
		ns tree.NodeStore,
		pool pool.BuffPool,
) (val.Tuple, error) {
	tb := val.NewTupleBuilder(toKeyDesc, ns)
	for i, fromHandler := range fromKeyDesc.Handlers {
		toHandler := toKeyDesc.Handlers[i]
		serialized, err := convertSerializedFkField(ctx, toHandler, fromHandler, key.GetField(i))
		if err != nil {
			return nil, err
		}

		switch toHandler.(type) {
		case val.AdaptiveEncodingTypeHandler:
			switch toKeyDesc.Types[i].Enc {
			case val.ExtendedAdaptiveEnc:
				err := tb.PutAdaptiveExtendedFromInline(ctx, i, serialized)
				if err != nil {
					return nil, err
				}
			case val.BytesAdaptiveEnc:
				err := tb.PutAdaptiveExtendedFromInline(ctx, i, serialized)
				if err != nil {
					return nil, err
				}
			default:
				panic(fmt.Sprintf("unexpected encoding for adaptive type: %d", fromKeyDesc.Types[i].Enc))
			}
		default:
			tb.PutRaw(i, serialized)
		}
	}

	var err error
	key, err = tb.Build(pool)
	if err != nil {
		return nil, err
	}
	return key, nil
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
