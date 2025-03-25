// Copyright 2021 Dolthub, Inc.
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

package durable

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/expression/function/vector"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// Index represents a Table index.
type Index interface {
	// HashOf returns the hash.Hash of this table.
	HashOf() (hash.Hash, error)

	// Count returns the cardinality of the index.
	Count() (uint64, error)

	// Empty returns true if the index is empty.
	Empty() (bool, error)

	// Format returns the types.NomsBinFormat for this index.
	Format() *types.NomsBinFormat

	// AddColumnToRows adds the column given to the rows data and returns the resulting rows.
	// The |newCol| is present in |newSchema|.
	AddColumnToRows(ctx context.Context, newCol string, newSchema schema.Schema) (Index, error)

	// Returns the serialized bytes of the (top of the) index.
	// Non-public. Used for flatbuffers Table persistence.
	bytes() ([]byte, error)

	DebugString(ctx context.Context, ns tree.NodeStore, schema schema.Schema) string
}

// IndexSet stores a collection secondary Indexes.
type IndexSet interface {
	// HashOf returns the hash.Hash of this table.
	HashOf() (hash.Hash, error)

	// GetIndex gets an index from the set.
	GetIndex(ctx context.Context, tableSch schema.Schema, idxSch schema.Schema, name string) (Index, error)

	// HasIndex returns true if an index with the specified name exists in the set.
	HasIndex(ctx context.Context, name string) (bool, error)

	// PutIndex puts an index into the set.
	PutIndex(ctx context.Context, name string, idx Index) (IndexSet, error)

	// PutNomsIndex puts a noms index into the set.
	// todo(andy): this is a temporary stop-gap while abstracting types.Map
	PutNomsIndex(ctx context.Context, name string, idx types.Map) (IndexSet, error)

	// DropIndex removes an index from the set.
	DropIndex(ctx context.Context, name string) (IndexSet, error)

	// RenameIndex renames index |oldName| to |newName|.
	RenameIndex(ctx context.Context, oldName, newName string) (IndexSet, error)
}

// RefFromIndex persists the Index and returns a types.Ref to it.
func RefFromIndex(ctx context.Context, vrw types.ValueReadWriter, idx Index) (types.Ref, error) {
	switch idx.Format() {
	case types.Format_LD_1:
		return refFromNomsValue(ctx, vrw, idx.(nomsIndex).index)

	case types.Format_DOLT:
		b := shim.ValueFromMap(MapFromIndex(idx))
		return refFromNomsValue(ctx, vrw, b)

	default:
		return types.Ref{}, errNbfUnknown
	}
}

// indexFromRef reads the types.Ref from storage and returns the Index it points to.
// This is only used by noms format and can be removed.
func indexFromRef(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, sch schema.Schema, r types.Ref) (Index, error) {
	return indexFromAddr(ctx, vrw, ns, sch, r.TargetHash(), false)
}

func indexFromAddr(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, sch schema.Schema, addr hash.Hash, isKeylessTable bool) (Index, error) {
	v, err := vrw.ReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}

	switch vrw.Format() {
	case types.Format_LD_1:
		return IndexFromNomsMap(v.(types.Map), vrw, ns), nil

	case types.Format_DOLT:
		m, err := shim.MapInterfaceFromValue(ctx, v, sch, ns, isKeylessTable)
		if err != nil {
			return nil, err
		}
		return IndexFromMapInterface(m), nil

	default:
		return nil, errNbfUnknown
	}
}

// NewEmptyPrimaryIndex creates a new empty Index for use as the primary index in a table.
func NewEmptyPrimaryIndex(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, indexSchema schema.Schema) (Index, error) {
	return newEmptyIndex(ctx, vrw, ns, indexSchema, false, false)
}

// NewEmptyForeignKeyIndex creates a new empty Index for use as a foreign key index.
// Foreign keys cannot appear on keyless tables.
func NewEmptyForeignKeyIndex(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, indexSchema schema.Schema) (Index, error) {
	return newEmptyIndex(ctx, vrw, ns, indexSchema, false, false)
}

// NewEmptyIndexFromTableSchema creates a new empty Index described by a schema.Index.
func NewEmptyIndexFromTableSchema(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, idx schema.Index, tableSchema schema.Schema) (Index, error) {
	indexSchema := idx.Schema()
	return newEmptyIndex(ctx, vrw, ns, indexSchema, idx.IsVector(), schema.IsKeyless(tableSchema))
}

// newEmptyIndex returns an index with no rows.
func newEmptyIndex(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, sch schema.Schema, isVector bool, isKeylessSecondary bool) (Index, error) {
	switch vrw.Format() {
	case types.Format_LD_1:
		m, err := types.NewMap(ctx, vrw)
		if err != nil {
			return nil, err
		}
		return IndexFromNomsMap(m, vrw, ns), nil

	case types.Format_DOLT:
		kd, vd := sch.GetMapDescriptors(ns)
		if isKeylessSecondary {
			kd = prolly.AddHashToSchema(kd)
		}
		if isVector {
			return NewEmptyProximityIndex(ctx, ns, kd, vd)
		} else {
			return NewEmptyProllyIndex(ctx, ns, kd, vd)
		}

	default:
		return nil, errNbfUnknown
	}
}

func NewEmptyProllyIndex(ctx context.Context, ns tree.NodeStore, kd, vd val.TupleDesc) (Index, error) {
	m, err := prolly.NewMapFromTuples(ctx, ns, kd, vd)
	if err != nil {
		return nil, err
	}
	return IndexFromProllyMap(m), nil
}

func NewEmptyProximityIndex(ctx context.Context, ns tree.NodeStore, kd, vd val.TupleDesc) (Index, error) {
	proximityMapBuilder, err := prolly.NewProximityMapBuilder(ctx, ns, vector.DistanceL2Squared{}, kd, vd, prolly.DefaultLogChunkSize)
	if err != nil {
		return nil, err
	}
	m, err := proximityMapBuilder.Flush(ctx)
	if err != nil {
		return nil, err
	}
	return IndexFromProximityMap(m), nil
}

type nomsIndex struct {
	index types.Map
	vrw   types.ValueReadWriter
	ns    tree.NodeStore
}

var _ Index = nomsIndex{}

func IterAllIndexes(
	ctx context.Context,
	sch schema.Schema,
	set IndexSet,
	cb func(name string, idx Index) error,
) error {
	for _, def := range sch.Indexes().AllIndexes() {
		idx, err := set.GetIndex(ctx, sch, nil, def.Name())
		if err != nil {
			return err
		}
		if err = cb(def.Name(), idx); err != nil {
			return err
		}
	}
	return nil
}

// NomsMapFromIndex unwraps the Index and returns the underlying types.Map.
func NomsMapFromIndex(i Index) types.Map {
	return i.(nomsIndex).index
}

// IndexFromNomsMap wraps a types.Map and returns it as an Index.
func IndexFromNomsMap(m types.Map, vrw types.ValueReadWriter, ns tree.NodeStore) Index {
	return nomsIndex{
		index: m,
		vrw:   vrw,
		ns:    ns,
	}
}

var _ Index = nomsIndex{}

// HashOf implements Index.
func (i nomsIndex) HashOf() (hash.Hash, error) {
	return i.index.Hash(i.vrw.Format())
}

// Count implements Index.
func (i nomsIndex) Count() (uint64, error) {
	return i.index.Len(), nil
}

// Empty implements Index.
func (i nomsIndex) Empty() (bool, error) {
	return i.index.Len() == 0, nil
}

// Format implements Index.
func (i nomsIndex) Format() *types.NomsBinFormat {
	return i.vrw.Format()
}

// bytes implements Index.
func (i nomsIndex) bytes() ([]byte, error) {
	rowschunk, err := types.EncodeValue(i.index, i.vrw.Format())
	if err != nil {
		return nil, err
	}
	return rowschunk.Data(), nil
}

func (i nomsIndex) AddColumnToRows(ctx context.Context, newCol string, newSchema schema.Schema) (Index, error) {
	// no-op for noms indexes because of tag-based mapping
	return i, nil
}

func (i nomsIndex) DebugString(ctx context.Context, ns tree.NodeStore, schema schema.Schema) string {
	panic("Not implemented")
}

type prollyIndex struct {
	index prolly.Map
}

// ProllyMapFromIndex unwraps the Index and returns the underlying prolly.Map.
func ProllyMapFromIndex(i Index) (prolly.Map, error) {
	switch i := i.(type) {
	case prollyIndex:
		return i.index, nil
	default:
		return prolly.Map{}, fmt.Errorf("expected prollyIndex, found: %T", i)
	}
}

// xxx: don't use this, temporary fix waiting for bigger
// fix in stats 2.0
func MaybeProllyMapFromIndex(i Index) (prolly.Map, bool) {
	ret, ok := i.(prollyIndex)
	if ok {
		return ret.index, true
	} else {
		return prolly.Map{}, false
	}
}

// MapFromIndex unwraps the Index and returns the underlying map as an interface.
func MapFromIndex(i Index) prolly.MapInterfaceWithMutable {
	switch indexType := i.(type) {
	case prollyIndex:
		return indexType.index
	case proximityIndex:
		return indexType.index
	}
	return i.(prollyIndex).index
}

// IndexFromProllyMap wraps a prolly.Map and returns it as an Index.
func IndexFromProllyMap(m prolly.Map) Index {
	return prollyIndex{index: m}
}

// IndexFromMapInterface wraps a prolly.MapInterface and returns it as an Index.
func IndexFromMapInterface(m prolly.MapInterface) Index {
	switch m := m.(type) {
	case prolly.Map:
		return IndexFromProllyMap(m)
	case prolly.ProximityMap:
		return IndexFromProximityMap(m)
	default:
		panic("unknown map type")
	}
}

var _ Index = prollyIndex{}

// HashOf implements Index.
func (i prollyIndex) HashOf() (hash.Hash, error) {
	return i.index.HashOf(), nil
}

// Count implements Index.
func (i prollyIndex) Count() (uint64, error) {
	c, err := i.index.Count()
	return uint64(c), err
}

// Empty implements Index.
func (i prollyIndex) Empty() (bool, error) {
	c, err := i.index.Count()
	if err != nil {
		return false, err
	}
	return c == 0, nil
}

// Format implements Index.
func (i prollyIndex) Format() *types.NomsBinFormat {
	return i.index.Format()
}

// bytes implements Index.
func (i prollyIndex) bytes() ([]byte, error) {
	return []byte(shim.ValueFromMap(i.index).(types.SerialMessage)), nil
}

var _ Index = prollyIndex{}

func (i prollyIndex) AddColumnToRows(ctx context.Context, newCol string, newSchema schema.Schema) (Index, error) {
	var last bool
	colIdx, iCol := 0, 0
	newSchema.GetNonPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		last = false
		if strings.EqualFold(col.Name, newCol) {
			last = true
			colIdx = iCol
		}
		iCol++
		return false, nil
	})

	// If the column we added was last among non-primary key columns we can skip this step
	if last {
		return i, nil
	}

	// If not, then we have to iterate over this table's rows and update all the offsets for the new column
	rowMap, err := ProllyMapFromIndex(i)
	if err != nil {
		return nil, err
	}
	mutator := rowMap.Mutate()

	iter, err := mutator.IterAll(ctx)
	if err != nil {
		return nil, err
	}

	// Re-write all the rows, inserting a zero-byte field in every value tuple
	_, valDesc := rowMap.Descriptors()
	b := val.NewTupleBuilder(valDesc, i.index.NodeStore())
	for {
		k, v, err := iter.Next(ctx)
		if err == io.EOF {
			b.Recycle()
			break
		} else if err != nil {
			return nil, err
		}

		for i := 0; i < colIdx; i++ {
			b.PutRaw(i, v.GetField(i))
		}
		b.PutRaw(colIdx, nil)
		for i := colIdx; i < v.Count(); i++ {
			b.PutRaw(i+1, v.GetField(i))
		}

		tup, err := b.BuildPermissive(sharePool, i.index.NodeStore())
		if err != nil {
			return nil, err
		}
		err = mutator.Put(ctx, k, tup)
		if err != nil {
			return nil, err
		}

		b.Recycle()
	}

	newMap, err := mutator.Map(ctx)
	if err != nil {
		return nil, err
	}

	return IndexFromProllyMap(newMap), nil
}

func (i prollyIndex) DebugString(ctx context.Context, ns tree.NodeStore, schema schema.Schema) string {
	var b bytes.Buffer
	i.index.WalkNodes(ctx, func(ctx context.Context, nd tree.Node) error {
		return tree.OutputProllyNode(ctx, &b, nd, ns, schema)
	})
	return b.String()
}

// NewIndexSet returns an empty IndexSet.
func NewIndexSet(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore) (IndexSet, error) {
	if vrw.Format().UsesFlatbuffers() {
		emptyam, err := prolly.NewEmptyAddressMap(ns)
		if err != nil {
			return nil, err
		}
		return doltDevIndexSet{vrw, ns, emptyam}, nil
	}

	empty, err := types.NewMap(ctx, vrw)
	if err != nil {
		return nil, err
	}
	return nomsIndexSet{
		indexes: empty,
		vrw:     vrw,
	}, nil
}

func NewIndexSetWithEmptyIndexes(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, sch schema.Schema) (IndexSet, error) {
	s, err := NewIndexSet(ctx, vrw, ns)
	if err != nil {
		return nil, err
	}
	for _, index := range sch.Indexes().AllIndexes() {
		empty, err := NewEmptyIndexFromTableSchema(ctx, vrw, ns, index, sch)
		if err != nil {
			return nil, err
		}
		s, err = s.PutIndex(ctx, index.Name(), empty)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

type nomsIndexSet struct {
	indexes types.Map
	vrw     types.ValueReadWriter
	ns      tree.NodeStore
}

var _ IndexSet = nomsIndexSet{}

// HashOf implements IndexSet.
func (s nomsIndexSet) HashOf() (hash.Hash, error) {
	return s.indexes.Hash(s.vrw.Format())
}

// HasIndex implements IndexSet.
func (s nomsIndexSet) HasIndex(ctx context.Context, name string) (bool, error) {
	_, ok, err := s.indexes.MaybeGet(ctx, types.String(name))
	if err != nil {
		return false, err
	}
	return ok, nil
}

// GetIndex implements IndexSet.
func (s nomsIndexSet) GetIndex(ctx context.Context, tableSch schema.Schema, idxSch schema.Schema, name string) (Index, error) {
	v, ok, err := s.indexes.MaybeGet(ctx, types.String(name))
	if !ok {
		err = fmt.Errorf("index %s not found in IndexSet", name)
	}
	if err != nil {
		return nil, err
	}

	idx := tableSch.Indexes().GetByName(name)
	if idx == nil {
		return nil, fmt.Errorf("index not found: %s", name)
	}

	return indexFromRef(ctx, s.vrw, s.ns, idxSch, v.(types.Ref))
}

// PutNomsIndex implements IndexSet.
func (s nomsIndexSet) PutNomsIndex(ctx context.Context, name string, idx types.Map) (IndexSet, error) {
	return s.PutIndex(ctx, name, IndexFromNomsMap(idx, s.vrw, s.ns))
}

// PutIndex implements IndexSet.
func (s nomsIndexSet) PutIndex(ctx context.Context, name string, idx Index) (IndexSet, error) {
	ref, err := RefFromIndex(ctx, s.vrw, idx)
	if err != nil {
		return nil, err
	}

	im, err := s.indexes.Edit().Set(types.String(name), ref).Map(ctx)
	if err != nil {
		return nil, err
	}

	return nomsIndexSet{indexes: im, vrw: s.vrw, ns: s.ns}, nil
}

// DropIndex implements IndexSet.
func (s nomsIndexSet) DropIndex(ctx context.Context, name string) (IndexSet, error) {
	im, err := s.indexes.Edit().Remove(types.String(name)).Map(ctx)
	if err != nil {
		return nil, err
	}

	return nomsIndexSet{indexes: im, vrw: s.vrw, ns: s.ns}, nil
}

func (s nomsIndexSet) RenameIndex(ctx context.Context, oldName, newName string) (IndexSet, error) {
	v, ok, err := s.indexes.MaybeGet(ctx, types.String(oldName))
	if !ok {
		err = fmt.Errorf("index %s not found in IndexSet", oldName)
	}
	if err != nil {
		return nil, err
	}

	edit := s.indexes.Edit()
	im, err := edit.Set(types.String(newName), v).Remove(types.String(oldName)).Map(ctx)
	if err != nil {
		return nil, err
	}

	return nomsIndexSet{indexes: im, vrw: s.vrw, ns: s.ns}, nil
}

func mapFromIndexSet(ic IndexSet) types.Map {
	return ic.(nomsIndexSet).indexes
}

type doltDevIndexSet struct {
	vrw types.ValueReadWriter
	ns  tree.NodeStore
	am  prolly.AddressMap
}

var _ IndexSet = doltDevIndexSet{}

func (is doltDevIndexSet) HashOf() (hash.Hash, error) {
	return is.am.HashOf(), nil
}

func (is doltDevIndexSet) HasIndex(ctx context.Context, targetName string) (bool, error) {
	addr, _, err := is.searchForCaseInsensitiveIndexName(ctx, targetName)
	return !addr.IsEmpty(), err
}

func (is doltDevIndexSet) GetIndex(ctx context.Context, tableSch schema.Schema, idxSch schema.Schema, name string) (Index, error) {
	foundAddr, _, err := is.searchForCaseInsensitiveIndexName(ctx, name)
	if err != nil {
		return nil, err
	}
	if foundAddr.IsEmpty() {
		return nil, fmt.Errorf("index %s not found in IndexSet", name)
	}

	idx := tableSch.Indexes().GetByName(name)
	if idx == nil {
		return nil, fmt.Errorf("index schema not found: %s", name)
	}
	if idxSch == nil {
		idxSch = idx.Schema()
	}
	return indexFromAddr(ctx, is.vrw, is.ns, idxSch, foundAddr, schema.IsKeyless(tableSch))
}

func (is doltDevIndexSet) PutIndex(ctx context.Context, name string, idx Index) (IndexSet, error) {
	ref, err := RefFromIndex(ctx, is.vrw, idx)
	if err != nil {
		return nil, err
	}

	ae := is.am.Editor()
	err = ae.Update(ctx, name, ref.TargetHash())
	if err != nil {
		return nil, err
	}
	am, err := ae.Flush(ctx)
	if err != nil {
		return nil, err
	}

	return doltDevIndexSet{is.vrw, is.ns, am}, nil
}

func (is doltDevIndexSet) PutNomsIndex(ctx context.Context, name string, idx types.Map) (IndexSet, error) {
	return is.PutIndex(ctx, name, IndexFromNomsMap(idx, is.vrw, is.ns))
}

func (is doltDevIndexSet) DropIndex(ctx context.Context, name string) (IndexSet, error) {
	foundAddr, foundName, err := is.searchForCaseInsensitiveIndexName(ctx, name)
	if err != nil {
		return nil, err
	}
	if foundAddr.IsEmpty() {
		return nil, fmt.Errorf("index %s not found in IndexSet", name)
	}

	ae := is.am.Editor()
	err = ae.Delete(ctx, foundName)
	if err != nil {
		return nil, err
	}
	am, err := ae.Flush(ctx)
	if err != nil {
		return nil, err
	}
	return doltDevIndexSet{is.vrw, is.ns, am}, nil
}

func (is doltDevIndexSet) RenameIndex(ctx context.Context, oldName, newName string) (IndexSet, error) {
	foundOldAddr, foundOldName, err := is.searchForCaseInsensitiveIndexName(ctx, oldName)
	if err != nil {
		return nil, err
	}
	if foundOldAddr.IsEmpty() {
		return nil, fmt.Errorf("index %s not found in IndexSet", oldName)
	}

	foundNewIndex, err := is.HasIndex(ctx, newName)
	if err != nil {
		return nil, err
	}
	if foundNewIndex {
		return nil, fmt.Errorf("index %s found in IndexSet when attempting to rename index", newName)
	}

	ae := is.am.Editor()
	err = ae.Update(ctx, newName, foundOldAddr)
	if err != nil {
		return nil, err
	}
	err = ae.Delete(ctx, foundOldName)
	if err != nil {
		return nil, err
	}

	am, err := ae.Flush(ctx)
	if err != nil {
		return nil, err
	}

	return doltDevIndexSet{is.vrw, is.ns, am}, nil
}

// searchForCaseInsensitiveIndexName searches through the index names in this index set looking for a case-insensitive
// match against |targetName|. If found, the address is returned, along with the exact case name. If no match was
// found, a nil address is returned, along with an empty string.
func (is doltDevIndexSet) searchForCaseInsensitiveIndexName(ctx context.Context, targetName string) (foundAddr hash.Hash, foundName string, err error) {
	// Indexes are stored with their original case name, so we have to iterate over the index names and
	// do a case-insensitive match to find a matching index
	err = is.am.IterAll(ctx, func(name string, address hash.Hash) error {
		if strings.EqualFold(name, targetName) {
			foundAddr = address
			foundName = name
		}
		return nil
	})
	if err != nil {
		return hash.Hash{}, "", err
	}
	return foundAddr, foundName, nil
}
