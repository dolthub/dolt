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

package fkconstrain

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

type fkCheck interface {
	ColsIntersectChanges(changes map[uint64]bool) bool
	Check(ctx context.Context, oldTV, newTV row.TaggedValues) error
}

type check struct {
	nbf                 *types.NomsBinFormat
	fk                  doltdb.ForeignKey
	declaredIndex       schema.Index
	declaredIndexRows   types.Map
	referencedIndex     schema.Index
	referencedIndexRows types.Map

	colTags           []uint64
	declTagsToRefTags map[uint64]uint64
	refTagsToDeclTags map[uint64]uint64
}

func newCheck(ctx context.Context, root *doltdb.RootValue, colTags []uint64, fk doltdb.ForeignKey) (check, error) {
	declTable, declSch, err := getTableAndSchema(ctx, root, fk.TableName)

	if err != nil {
		return check{}, err
	}

	refTable, refSch, err := getTableAndSchema(ctx, root, fk.ReferencedTableName)

	if err != nil {
		return check{}, err
	}

	refIdx := refSch.Indexes().GetByName(fk.ReferencedTableIndex)
	refIdxRowData, err := refTable.GetIndexRowData(ctx, fk.ReferencedTableIndex)

	if err != nil {
		return check{}, err
	}

	declIdx := declSch.Indexes().GetByName(fk.TableIndex)
	declIdxRowData, err := declTable.GetIndexRowData(ctx, fk.TableIndex)

	if err != nil {
		return check{}, err
	}

	declTagsToRefTags := make(map[uint64]uint64)
	refTagsToDeclTags := make(map[uint64]uint64)
	for i, declTag := range fk.TableColumns {
		refTag := fk.ReferencedTableColumns[i]
		declTagsToRefTags[declTag] = refTag
		refTagsToDeclTags[refTag] = declTag
	}

	return check{
		nbf:                 root.VRW().Format(),
		fk:                  fk,
		declaredIndex:       declIdx,
		declaredIndexRows:   declIdxRowData,
		referencedIndex:     refIdx,
		referencedIndexRows: refIdxRowData,
		colTags:             colTags,
		declTagsToRefTags:   declTagsToRefTags,
		refTagsToDeclTags:   refTagsToDeclTags,
	}, nil
}

func (chk check) ColsIntersectChanges(changes map[uint64]bool) bool {
	for _, tag := range chk.colTags {
		if changes[tag] {
			return true
		}
	}

	return false
}

func (chk check) NewErrForKey(key types.Tuple) error {
	return &GenericForeignKeyError{
		tableName:           chk.fk.TableName,
		referencedTableName: chk.fk.ReferencedTableName,
		fkName:              chk.fk.Name,
		keyStr:              key.String(),
	}
}

type declaredFKCheck struct {
	check
}

func newDeclaredFKCheck(ctx context.Context, root *doltdb.RootValue, fk doltdb.ForeignKey) (declaredFKCheck, error) {
	chk, err := newCheck(ctx, root, fk.TableColumns, fk)

	if err != nil {
		return declaredFKCheck{}, err
	}

	return declaredFKCheck{
		check: chk,
	}, nil
}

// Check checks that the new tagged values coming from the declared table are present in the referenced index
func (declFKC declaredFKCheck) Check(ctx context.Context, _, newTV row.TaggedValues) error {
	indexColTags := declFKC.referencedIndex.IndexedColumnTags()
	keyTupVals := make([]types.Value, len(indexColTags)*2)
	for i, refTag := range indexColTags {
		declTag := declFKC.refTagsToDeclTags[refTag]
		keyTupVals[i*2] = types.Uint(refTag)

		if val, ok := newTV[declTag]; ok && !types.IsNull(val) {
			keyTupVals[i*2+1] = val
		} else {
			// full key is not present.  skip check
			return nil
		}
	}

	key, err := types.NewTuple(declFKC.nbf, keyTupVals...)

	if err != nil {
		return err
	}

	found, err := indexHasKey(ctx, declFKC.referencedIndexRows, key)

	if err != nil {
		return err
	}

	if !found {
		return declFKC.NewErrForKey(key)
	}

	return nil
}

type referencedFKCheck struct {
	check
}

func newRefFKCheck(ctx context.Context, root *doltdb.RootValue, fk doltdb.ForeignKey) (referencedFKCheck, error) {
	chk, err := newCheck(ctx, root, fk.ReferencedTableColumns, fk)

	if err != nil {
		return referencedFKCheck{}, err
	}

	return referencedFKCheck{
		check: chk,
	}, nil
}

// Check checks that either the value coming from the old tagged values is present in a new row in the referenced index
// or the value is no longer referenced by rows in the declared index.
func (refFKC referencedFKCheck) Check(ctx context.Context, oldTV, _ row.TaggedValues) error {
	indexColTags := refFKC.referencedIndex.IndexedColumnTags()
	keyTupVals := make([]types.Value, len(refFKC.fk.ReferencedTableColumns)*2)
	for i, tag := range indexColTags {
		keyTupVals[i*2] = types.Uint(tag)

		if val, ok := oldTV[tag]; ok && !types.IsNull(val) {
			keyTupVals[i*2+1] = val
		} else {
			// full key is not present.  skip check
			return nil
		}
	}

	key, err := types.NewTuple(refFKC.nbf, keyTupVals...)

	if err != nil {
		return err
	}

	found, err := indexHasKey(ctx, refFKC.referencedIndexRows, key)

	if err != nil {
		return err
	}

	if found {
		return nil
	}

	// If there is not a new value with the old key then make sure no rows in the table point to the old value
	declIndexTags := refFKC.declaredIndex.IndexedColumnTags()
	keyTupVals = make([]types.Value, len(indexColTags)*2)
	for i, declTag := range declIndexTags {
		refTag := refFKC.declTagsToRefTags[declTag]
		keyTupVals[i*2] = types.Uint(declTag)

		if val, ok := oldTV[refTag]; ok {
			keyTupVals[i*2+1] = val
		} else {
			keyTupVals[i*2+1] = types.NullValue
		}
	}

	key, err = types.NewTuple(refFKC.nbf, keyTupVals...)

	if err != nil {
		return err
	}

	found, err = indexHasKey(ctx, refFKC.declaredIndexRows, key)

	if err != nil {
		return err
	}

	if found {
		// found a row referencing a key that no longer exists
		return refFKC.NewErrForKey(key)
	}

	return nil
}

func indexHasKey(ctx context.Context, indexRows types.Map, key types.Tuple) (bool, error) {
	itr, err := indexRows.IteratorFrom(ctx, key)

	if err != nil {
		return false, err
	}

	refKey, _, err := itr.NextTuple(ctx)

	if err == io.EOF {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return refKey.StartsWith(key), nil
}
