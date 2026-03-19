// Copyright 2026 Dolthub, Inc.
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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// TestFkIdxKeyDescs_FkColNotAtFront verifies that fkIdxKeyDescs returns a prefix descriptor
// with handlers in FK column order when the FK column is not at position 0 in the child's
// primary key.
//
// The test creates real Dolt tables so that IndexData is obtained through the same calls
// newConstraintViolationsLoadedTable makes: GetIndexRowData for the child secondary index
// and GetRowData for the primary and parent indexes.
//
// Because Dolt does not assign TupleTypeHandlers for standard column types, the test
// re-wraps each production TupleDesc with custom handlers injected at positions determined
// by AllTags. This makes handler-position mismatches observable without altering the
// type ordering that came from the production path.
func TestFkIdxKeyDescs_FkColNotAtFront(t *testing.T) {
	ctx := context.Background()
	ddb := dtestutils.CreateTestEnv().DoltDB(ctx)
	vrw := ddb.ValueReadWriter()
	ns := ddb.NodeStore()

	handlerA := fkTestHandler{id: 1}
	handlerB := fkTestHandler{id: 2}

	const (
		pk1Tag   uint64 = 1
		pk2Tag   uint64 = 2
		fkColTag uint64 = 3
		parentPk uint64 = 4
	)
	tagToHandler := map[uint64]val.TupleTypeHandler{
		fkColTag: handlerB,
		pk1Tag:   handlerA,
		pk2Tag:   handlerA,
		parentPk: handlerA,
	}

	// Build child schema: PRIMARY KEY(pk1 INT, pk2 INT), fk_col VARCHAR.
	childCols := schema.NewColCollection(
		mustCol(t, "pk1", pk1Tag, typeinfo.Int32Type, true),
		mustCol(t, "pk2", pk2Tag, typeinfo.Int32Type, true),
		mustCol(t, "fk_col", fkColTag, typeinfo.StringDefaultType, false),
	)
	childSch := schema.MustSchemaFromCols(childCols)

	// AddIndexByColTags calls combineAllTags([fkColTag], [pk1Tag, pk2Tag]) internally,
	// producing allTags = [fkColTag, pk1Tag, pk2Tag]. fk_col is third in the schema
	// definition above but moves to position 0 in AllTags because it is the indexed column.
	const childIdxName = "idx_fk_col"
	childSchIdx, err := childSch.Indexes().AddIndexByColTags(childIdxName, []uint64{fkColTag}, nil, schema.IndexProperties{IsUserDefined: true})
	require.NoError(t, err)
	require.Equal(t, fkColTag, childSchIdx.AllTags()[0],
		"fk_col must be at position 0 in AllTags even though it is third in the schema definition")

	parentCols := schema.NewColCollection(
		mustCol(t, "pk", parentPk, typeinfo.Int32Type, true),
	)
	parentSch := schema.MustSchemaFromCols(parentCols)

	childTbl, err := doltdb.NewEmptyTable(ctx, vrw, ns, childSch)
	require.NoError(t, err)
	parentTbl, err := doltdb.NewEmptyTable(ctx, vrw, ns, parentSch)
	require.NoError(t, err)

	// Load IndexData the same way newConstraintViolationsLoadedTable does.
	childSecIdxData, err := childTbl.GetIndexRowData(ctx, childIdxName)
	require.NoError(t, err)
	childPriIdxData, err := childTbl.GetRowData(ctx)
	require.NoError(t, err)
	parentIdxData, err := parentTbl.GetRowData(ctx)
	require.NoError(t, err)

	// Inject custom handlers into each production TupleDesc. The type ordering comes from
	// the production path. AllTags drives handler assignment for the secondary index so that
	// the FK column's handler occupies the position combineAllTags placed it at.
	childSecIdxData = withHandlers(t, ctx, ns, childSecIdxData, handlersFromTags(childSchIdx.AllTags(), tagToHandler))
	childPriIdxData = withHandlers(t, ctx, ns, childPriIdxData, handlersFromTags([]uint64{pk1Tag, pk2Tag}, tagToHandler))
	parentIdxData = withHandlers(t, ctx, ns, parentIdxData, handlersFromTags([]uint64{parentPk}, tagToHandler))

	fkColCount := 1

	_, _, parentPrefixDesc, err := fkIdxKeyDescs(parentIdxData, fkColCount)
	require.NoError(t, err)

	_, childSecFullDesc, childFkColsDesc, err := fkIdxKeyDescs(childSecIdxData, fkColCount)
	require.NoError(t, err)

	assert.Greater(t, len(childSecFullDesc.Handlers), len(childFkColsDesc.Handlers),
		"prefix descriptor must be shorter than the full secondary index descriptor")
	assert.Equal(t, len(childFkColsDesc.Handlers), len(parentPrefixDesc.Handlers),
		"child and parent prefix descriptors must have equal length")
	assert.False(t,
		fkHandlersAreSerializationCompatible(childFkColsDesc, parentPrefixDesc),
		"fk_col (handlerB) is not compatible with parent.pk (handlerA): conversion required",
	)

	// Using the primary index instead produces a false positive: pk1 sits at position 0
	// with handlerA, which matches parent.pk (handlerA), so the check reports compatible
	// and conversion is silently skipped.
	_, _, childPriPrefixDesc, err := fkIdxKeyDescs(childPriIdxData, fkColCount)
	require.NoError(t, err)
	assert.True(t,
		fkHandlersAreSerializationCompatible(childPriPrefixDesc, parentPrefixDesc),
		"pk1 (handlerA) matches parent.pk (handlerA): primary index produces a false positive, fk_col conversion is incorrectly skipped",
	)
}

// handlersFromTags returns a handler slice ordered by |tags|, looking each up in |tagToHandler|.
func handlersFromTags(tags []uint64, tagToHandler map[uint64]val.TupleTypeHandler) []val.TupleTypeHandler {
	handlers := make([]val.TupleTypeHandler, len(tags))
	for position, tag := range tags {
		handlers[position] = tagToHandler[tag]
	}
	return handlers
}

// withHandlers re-wraps |index| as a new [durable.Index] whose key TupleDesc carries |handlers|.
// The type encodings are taken from the existing descriptor so the production column ordering
// is preserved.
func withHandlers(t *testing.T, ctx context.Context, nodeStore tree.NodeStore, index durable.Index, handlers []val.TupleTypeHandler) durable.Index {
	t.Helper()
	prollyMap, err := durable.ProllyMapFromIndex(index)
	require.NoError(t, err)
	keyDesc, valDesc := prollyMap.Descriptors()
	patchedKeyDesc := val.NewTupleDescriptorWithArgs(val.TupleDescriptorArgs{Handlers: handlers}, keyDesc.Types...)
	patchedMap, err := prolly.NewMapFromTuples(ctx, nodeStore, patchedKeyDesc, valDesc)
	require.NoError(t, err)
	return durable.IndexFromProllyMap(patchedMap)
}

// mustCol creates a [schema.Column] with the given typeinfo, panicking on error.
func mustCol(t *testing.T, name string, tag uint64, ti typeinfo.TypeInfo, partOfPK bool) schema.Column {
	t.Helper()
	col, err := schema.NewColumnWithTypeInfo(name, tag, ti, partOfPK, "", false, "")
	require.NoError(t, err)
	return col
}

// fkTestHandler is a minimal [val.TupleTypeHandler] whose SerializationCompatible returns true
// only when both handlers share the same id.
type fkTestHandler struct{ id int }

func (m fkTestHandler) SerializedCompare(_ context.Context, _, _ []byte) (int, error) {
	return 0, nil
}
func (m fkTestHandler) SerializeValue(_ context.Context, _ any) ([]byte, error) {
	return nil, nil
}
func (m fkTestHandler) DeserializeValue(_ context.Context, _ []byte) (any, error) {
	return nil, nil
}
func (m fkTestHandler) FormatValue(_ any) (string, error) { return "", nil }
func (m fkTestHandler) SerializationCompatible(other val.TupleTypeHandler) bool {
	o, ok := other.(fkTestHandler)
	return ok && o.id == m.id
}
func (m fkTestHandler) ConvertSerialized(_ context.Context, _ val.TupleTypeHandler, v []byte) ([]byte, error) {
	return v, nil
}
