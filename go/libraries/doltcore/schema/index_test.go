// Copyright 2020 Dolthub, Inc.
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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestIndexCollectionAddIndex(t *testing.T) {
	colColl := NewColCollection(
		NewColumn("pk1", 1, types.IntKind, true, NotNullConstraint{}),
		NewColumn("pk2", 2, types.IntKind, true, NotNullConstraint{}),
		NewColumn("v1", 3, types.IntKind, false),
		NewColumn("v2", 4, types.UintKind, false),
		NewColumn("v3", 5, types.StringKind, false),
	)
	indexColl := NewIndexCollection(colColl, nil).(*indexCollectionImpl)

	testIndexes := []*indexImpl{
		{
			name:      "idx_v1",
			tags:      []uint64{3},
			allTags:   []uint64{3, 1, 2},
			indexColl: indexColl,
		},
		{
			name:      "idx_v1v3v2",
			tags:      []uint64{3, 5, 4},
			allTags:   []uint64{3, 5, 4, 1, 2},
			indexColl: indexColl,
			comment:   "hello there",
		},
		{
			name:      "idx_pk1v1",
			tags:      []uint64{1, 3},
			allTags:   []uint64{1, 3, 2},
			indexColl: indexColl,
		},
		{
			name:      "idx_pk2pk1v2",
			tags:      []uint64{2, 1, 4},
			allTags:   []uint64{2, 1, 4},
			indexColl: indexColl,
		},
	}

	for _, testIndex := range testIndexes {
		t.Run(testIndex.Name(), func(t *testing.T) {
			assert.False(t, indexColl.Contains(testIndex.Name()))
			assert.False(t, indexColl.hasIndexOnColumns(testIndex.ColumnNames()...))
			assert.False(t, indexColl.hasIndexOnTags(testIndex.IndexedColumnTags()...))
			assert.Nil(t, indexColl.GetByName(testIndex.Name()))

			indexColl.AddIndex(testIndex)
			assert.Equal(t, testIndex, indexColl.GetByName(testIndex.Name()))
			assert.Equal(t, []Index{testIndex}, indexColl.AllIndexes())
			for _, tag := range testIndex.IndexedColumnTags() {
				assert.Equal(t, []Index{testIndex}, indexColl.IndexesWithTag(tag))
			}
			for _, col := range testIndex.ColumnNames() {
				assert.Equal(t, []Index{testIndex}, indexColl.IndexesWithColumn(col))
			}
			assert.True(t, indexColl.Contains(testIndex.Name()))
			assert.True(t, indexColl.hasIndexOnColumns(testIndex.ColumnNames()...))
			assert.True(t, indexColl.hasIndexOnTags(testIndex.IndexedColumnTags()...))
		})
		indexColl.clear(t)
	}

	const prefix = "new_"

	t.Run("Tag Overwrites", func(t *testing.T) {
		for _, testIndex := range testIndexes {
			indexColl.AddIndex(testIndex)
			newIndex := testIndex.copy()
			newIndex.name = prefix + testIndex.name
			indexColl.AddIndex(newIndex)
			assert.Equal(t, newIndex, indexColl.GetByName(newIndex.Name()))
			assert.Nil(t, indexColl.GetByName(testIndex.Name()))
			assert.Contains(t, indexColl.AllIndexes(), newIndex)
			assert.NotContains(t, indexColl.AllIndexes(), testIndex)
			for _, tag := range newIndex.IndexedColumnTags() {
				assert.Contains(t, indexColl.IndexesWithTag(tag), newIndex)
				assert.NotContains(t, indexColl.IndexesWithTag(tag), testIndex)
			}
			for _, col := range newIndex.ColumnNames() {
				assert.Contains(t, indexColl.IndexesWithColumn(col), newIndex)
				assert.NotContains(t, indexColl.IndexesWithColumn(col), testIndex)
			}
			assert.True(t, indexColl.Contains(newIndex.Name()))
			assert.False(t, indexColl.Contains(testIndex.Name()))
			assert.True(t, indexColl.hasIndexOnColumns(newIndex.ColumnNames()...))
			assert.True(t, indexColl.hasIndexOnTags(newIndex.IndexedColumnTags()...))
		}
	})

	t.Run("Name Overwrites", func(t *testing.T) {
		// should be able to reduce collection to one index
		lastStanding := &indexImpl{
			name:      "none",
			tags:      []uint64{4},
			allTags:   []uint64{4, 1, 2},
			indexColl: indexColl,
		}

		for _, testIndex := range testIndexes {
			lastStanding.name = prefix + testIndex.name
			indexColl.AddIndex(lastStanding)
		}

		assert.Equal(t, map[string]*indexImpl{lastStanding.name: lastStanding}, indexColl.indexes)
		for tag, indexes := range indexColl.colTagToIndex {
			if tag == 4 {
				assert.Equal(t, indexes, []*indexImpl{lastStanding})
			} else {
				assert.Empty(t, indexes)
			}
		}
	})
}

func TestIndexCollectionAddIndexByColNames(t *testing.T) {
	colColl := NewColCollection(
		NewColumn("pk1", 1, types.IntKind, true, NotNullConstraint{}),
		NewColumn("pk2", 2, types.IntKind, true, NotNullConstraint{}),
		NewColumn("v1", 3, types.IntKind, false),
		NewColumn("v2", 4, types.UintKind, false),
		NewColumn("v3", 5, types.StringKind, false),
	)
	indexColl := NewIndexCollection(colColl, nil).(*indexCollectionImpl)

	testIndexes := []struct {
		cols  []string
		index *indexImpl
	}{
		{
			[]string{"v1"},
			&indexImpl{
				name:      "idx_v1",
				tags:      []uint64{3},
				allTags:   []uint64{3, 1, 2},
				indexColl: indexColl,
			},
		},
		{
			[]string{"v1", "v3", "v2"},
			&indexImpl{
				name:      "idx_v1v3v2",
				tags:      []uint64{3, 5, 4},
				allTags:   []uint64{3, 5, 4, 1, 2},
				indexColl: indexColl,
			},
		},
		{
			[]string{"pk1", "v1"},
			&indexImpl{
				name:      "idx_pk1v1",
				tags:      []uint64{1, 3},
				allTags:   []uint64{1, 3, 2},
				indexColl: indexColl,
				comment:   "hello there",
			},
		},
		{
			[]string{"pk2", "pk1", "v2"},
			&indexImpl{
				name:      "idx_pk2pk1v2",
				tags:      []uint64{2, 1, 4},
				allTags:   []uint64{2, 1, 4},
				indexColl: indexColl,
			},
		},
	}

	for _, testIndex := range testIndexes {
		t.Run(testIndex.index.Name(), func(t *testing.T) {
			assert.False(t, indexColl.Contains(testIndex.index.Name()))
			assert.False(t, indexColl.hasIndexOnColumns(testIndex.index.ColumnNames()...))
			assert.False(t, indexColl.hasIndexOnTags(testIndex.index.IndexedColumnTags()...))
			assert.Nil(t, indexColl.GetByName(testIndex.index.Name()))

			resIndex, err := indexColl.AddIndexByColNames(testIndex.index.Name(), testIndex.cols, IndexProperties{IsUnique: testIndex.index.IsUnique(), Comment: testIndex.index.Comment()})
			assert.NoError(t, err)
			assert.Equal(t, testIndex.index, resIndex)
			assert.Equal(t, testIndex.index, indexColl.GetByName(resIndex.Name()))
			assert.Equal(t, []Index{testIndex.index}, indexColl.AllIndexes())
			for _, tag := range resIndex.IndexedColumnTags() {
				assert.Equal(t, []Index{resIndex}, indexColl.IndexesWithTag(tag))
			}
			for _, col := range resIndex.ColumnNames() {
				assert.Equal(t, []Index{resIndex}, indexColl.IndexesWithColumn(col))
			}
			assert.True(t, indexColl.Contains(resIndex.Name()))
			assert.True(t, indexColl.hasIndexOnColumns(resIndex.ColumnNames()...))
			assert.True(t, indexColl.hasIndexOnTags(resIndex.IndexedColumnTags()...))
		})
		indexColl.clear(t)
	}

	t.Run("Pre-existing", func(t *testing.T) {
		for _, testIndex := range testIndexes {
			_, err := indexColl.AddIndexByColNames(testIndex.index.Name(), testIndex.cols, IndexProperties{IsUnique: testIndex.index.IsUnique(), Comment: testIndex.index.Comment()})
			assert.NoError(t, err)
			_, err = indexColl.AddIndexByColNames(testIndex.index.Name()+"copy", testIndex.cols, IndexProperties{IsUnique: testIndex.index.IsUnique(), Comment: testIndex.index.Comment()})
			assert.NoError(t, err)
			_, err = indexColl.AddIndexByColNames(testIndex.index.Name(), []string{"v2"}, IndexProperties{IsUnique: testIndex.index.IsUnique(), Comment: testIndex.index.Comment()})
			assert.Error(t, err)
		}
		indexColl.clear(t)
	})

	t.Run("Non-existing Columns", func(t *testing.T) {
		_, err := indexColl.AddIndexByColNames("nonsense", []string{"v4"}, IndexProperties{IsUnique: false, Comment: ""})
		assert.Error(t, err)
		_, err = indexColl.AddIndexByColNames("nonsense", []string{"v1", "v2", "pk3"}, IndexProperties{IsUnique: false, Comment: ""})
		assert.Error(t, err)
	})
}

func TestIndexCollectionAddIndexByColTags(t *testing.T) {
	colColl := NewColCollection(
		NewColumn("pk1", 1, types.IntKind, true, NotNullConstraint{}),
		NewColumn("pk2", 2, types.IntKind, true, NotNullConstraint{}),
		NewColumn("v1", 3, types.IntKind, false),
		NewColumn("v2", 4, types.UintKind, false),
		NewColumn("v3", 5, types.StringKind, false),
	)
	indexColl := NewIndexCollection(colColl, nil).(*indexCollectionImpl)

	testIndexes := []*indexImpl{
		{
			name:      "idx_v1",
			tags:      []uint64{3},
			allTags:   []uint64{3, 1, 2},
			indexColl: indexColl,
			comment:   "hello there",
		},
		{
			name:      "idx_v1v3v2",
			tags:      []uint64{3, 5, 4},
			allTags:   []uint64{3, 5, 4, 1, 2},
			indexColl: indexColl,
		},
		{
			name:      "idx_pk1v1",
			tags:      []uint64{1, 3},
			allTags:   []uint64{1, 3, 2},
			indexColl: indexColl,
		},
		{
			name:      "idx_pk2pk1v2",
			tags:      []uint64{2, 1, 4},
			allTags:   []uint64{2, 1, 4},
			indexColl: indexColl,
		},
	}

	for _, testIndex := range testIndexes {
		t.Run(testIndex.Name(), func(t *testing.T) {
			assert.False(t, indexColl.Contains(testIndex.Name()))
			assert.False(t, indexColl.hasIndexOnColumns(testIndex.ColumnNames()...))
			assert.False(t, indexColl.hasIndexOnTags(testIndex.IndexedColumnTags()...))
			assert.Nil(t, indexColl.GetByName(testIndex.Name()))

			resIndex, err := indexColl.AddIndexByColTags(testIndex.Name(), testIndex.tags, IndexProperties{IsUnique: testIndex.IsUnique(), Comment: testIndex.Comment()})
			assert.NoError(t, err)
			assert.Equal(t, testIndex, resIndex)
			assert.Equal(t, testIndex, indexColl.GetByName(resIndex.Name()))
			assert.Equal(t, []Index{testIndex}, indexColl.AllIndexes())
			for _, tag := range resIndex.IndexedColumnTags() {
				assert.Equal(t, []Index{resIndex}, indexColl.IndexesWithTag(tag))
			}
			for _, col := range resIndex.ColumnNames() {
				assert.Equal(t, []Index{resIndex}, indexColl.IndexesWithColumn(col))
			}
			assert.True(t, indexColl.Contains(resIndex.Name()))
			assert.True(t, indexColl.hasIndexOnColumns(resIndex.ColumnNames()...))
			assert.True(t, indexColl.hasIndexOnTags(resIndex.IndexedColumnTags()...))
		})
		indexColl.clear(t)
	}

	t.Run("Pre-existing", func(t *testing.T) {
		for _, testIndex := range testIndexes {
			_, err := indexColl.AddIndexByColTags(testIndex.Name(), testIndex.tags, IndexProperties{IsUnique: testIndex.IsUnique(), Comment: testIndex.Comment()})
			assert.NoError(t, err)
			_, err = indexColl.AddIndexByColTags(testIndex.Name()+"copy", testIndex.tags, IndexProperties{IsUnique: testIndex.IsUnique(), Comment: testIndex.Comment()})
			assert.NoError(t, err)
			_, err = indexColl.AddIndexByColTags(testIndex.Name(), []uint64{4}, IndexProperties{IsUnique: testIndex.IsUnique(), Comment: testIndex.Comment()})
			assert.Error(t, err)
		}
		indexColl.clear(t)
	})

	t.Run("Non-existing Tags", func(t *testing.T) {
		_, err := indexColl.AddIndexByColTags("nonsense", []uint64{6}, IndexProperties{IsUnique: false, Comment: ""})
		assert.Error(t, err)
		_, err = indexColl.AddIndexByColTags("nonsense", []uint64{3, 4, 10}, IndexProperties{IsUnique: false, Comment: ""})
		assert.Error(t, err)
	})
}

func TestIndexCollectionAllIndexes(t *testing.T) {
	colColl := NewColCollection(
		NewColumn("pk1", 1, types.IntKind, true, NotNullConstraint{}),
		NewColumn("pk2", 2, types.IntKind, true, NotNullConstraint{}),
		NewColumn("v1", 3, types.IntKind, false),
		NewColumn("v2", 4, types.UintKind, false),
		NewColumn("v3", 5, types.StringKind, false),
	)
	indexColl := NewIndexCollection(colColl, nil).(*indexCollectionImpl)

	indexColl.AddIndex(&indexImpl{
		name: "idx_z",
		tags: []uint64{3},
	})
	_, err := indexColl.AddIndexByColNames("idx_a", []string{"v2"}, IndexProperties{IsUnique: false, Comment: ""})
	require.NoError(t, err)
	_, err = indexColl.AddIndexByColTags("idx_n", []uint64{5}, IndexProperties{IsUnique: false, Comment: "hello there"})
	require.NoError(t, err)

	assert.Equal(t, []Index{
		&indexImpl{
			name:      "idx_a",
			tags:      []uint64{4},
			allTags:   []uint64{4, 1, 2},
			indexColl: indexColl,
			isUnique:  false,
			comment:   "",
		},
		&indexImpl{
			name:      "idx_n",
			tags:      []uint64{5},
			allTags:   []uint64{5, 1, 2},
			indexColl: indexColl,
			isUnique:  false,
			comment:   "hello there",
		},
		&indexImpl{
			name:      "idx_z",
			tags:      []uint64{3},
			allTags:   []uint64{3, 1, 2},
			indexColl: indexColl,
			isUnique:  false,
			comment:   "",
		},
	}, indexColl.AllIndexes())
}

func TestIndexCollectionRemoveIndex(t *testing.T) {
	colColl := NewColCollection(
		NewColumn("pk1", 1, types.IntKind, true, NotNullConstraint{}),
		NewColumn("pk2", 2, types.IntKind, true, NotNullConstraint{}),
		NewColumn("v1", 3, types.IntKind, false),
		NewColumn("v2", 4, types.UintKind, false),
		NewColumn("v3", 5, types.StringKind, false),
	)
	indexColl := NewIndexCollection(colColl, nil).(*indexCollectionImpl)

	testIndexes := []Index{
		&indexImpl{
			name:      "idx_v1",
			tags:      []uint64{3},
			allTags:   []uint64{3, 1, 2},
			indexColl: indexColl,
		},
		&indexImpl{
			name:      "idx_v1v3v2",
			tags:      []uint64{3, 5, 4},
			allTags:   []uint64{3, 5, 4, 1, 2},
			indexColl: indexColl,
			comment:   "hello there",
		},
		&indexImpl{
			name:      "idx_pk1v1",
			tags:      []uint64{1, 3},
			allTags:   []uint64{1, 3, 2},
			indexColl: indexColl,
		},
		&indexImpl{
			name:      "idx_pk2pk1v2",
			tags:      []uint64{2, 1, 4},
			allTags:   []uint64{2, 1, 4},
			indexColl: indexColl,
		},
	}
	indexColl.AddIndex(testIndexes...)

	for _, testIndex := range testIndexes {
		resIndex, err := indexColl.RemoveIndex(testIndex.Name())
		assert.NoError(t, err)
		assert.Equal(t, testIndex, resIndex)
		assert.NotContains(t, indexColl.indexes, resIndex.Name())
		assert.NotContains(t, indexColl.indexes, resIndex)
		for _, indexes := range indexColl.colTagToIndex {
			assert.NotContains(t, indexes, resIndex)
		}
		_, err = indexColl.RemoveIndex(testIndex.Name())
		assert.Error(t, err)
	}
}

func TestIndexCollectionRenameIndex(t *testing.T) {
	colColl := NewColCollection(
		NewColumn("pk1", 1, types.IntKind, true, NotNullConstraint{}),
		NewColumn("pk2", 2, types.IntKind, true, NotNullConstraint{}),
		NewColumn("v1", 3, types.IntKind, false),
		NewColumn("v2", 4, types.UintKind, false),
		NewColumn("v3", 5, types.StringKind, false),
	)
	indexColl := NewIndexCollection(colColl, nil).(*indexCollectionImpl)
	index := &indexImpl{
		name:      "idx_a",
		tags:      []uint64{3},
		allTags:   []uint64{3, 1, 2},
		indexColl: indexColl,
	}
	indexColl.AddIndex(index)

	const newIndexName = "idx_newname"
	expectedIndex := index.copy()
	expectedIndex.name = newIndexName

	resIndex, err := indexColl.RenameIndex(index.Name(), newIndexName)
	newIndex := resIndex.(*indexImpl)
	assert.NoError(t, err)
	assert.Equal(t, expectedIndex, resIndex)
	assert.Equal(t, indexColl.indexes, map[string]*indexImpl{newIndexName: newIndex})
	for tag, indexes := range indexColl.colTagToIndex {
		if tag == 3 {
			assert.Equal(t, indexes, []*indexImpl{newIndex})
		} else {
			assert.Empty(t, indexes)
		}
	}

	indexColl.AddIndex(index)
	_, err = indexColl.RenameIndex(newIndexName, index.Name())
	assert.Error(t, err)
}

func TestIndexCollectionDuplicateIndexes(t *testing.T) {
	colColl := NewColCollection(
		NewColumn("pk1", 1, types.IntKind, true, NotNullConstraint{}),
		NewColumn("pk2", 2, types.IntKind, true, NotNullConstraint{}),
		NewColumn("v1", 3, types.IntKind, false),
		NewColumn("v2", 4, types.UintKind, false),
		NewColumn("v3", 5, types.StringKind, false),
	)
	indexColl := NewIndexCollection(colColl, nil).(*indexCollectionImpl)

	// Create original index
	origIndex := struct {
		cols  []string
		index *indexImpl
	}{
		[]string{"v1"},
		&indexImpl{
			name:      "v1_orig",
			tags:      []uint64{3},
			allTags:   []uint64{3, 1, 2},
			indexColl: indexColl,
		},
	}

	// Create duplicate index
	copyIndex := struct {
		cols  []string
		index *indexImpl
	}{
		[]string{"v1"},
		&indexImpl{
			name:      "v1_copy",
			tags:      []uint64{3},
			allTags:   []uint64{3, 1, 2},
			indexColl: indexColl,
		},
	}

	// Check that indexColl doesn't yet contain origIndex
	assert.False(t, indexColl.Contains(origIndex.index.Name()))
	assert.False(t, indexColl.hasIndexOnColumns(origIndex.index.ColumnNames()...))
	assert.False(t, indexColl.hasIndexOnTags(origIndex.index.IndexedColumnTags()...))
	assert.Nil(t, indexColl.GetByName(origIndex.index.Name()))

	// Insert origIndex and see that no errors occur
	resOrigIndex, err := indexColl.AddIndexByColNames(origIndex.index.Name(), origIndex.cols, IndexProperties{IsUnique: origIndex.index.IsUnique(), Comment: origIndex.index.Comment()})
	assert.NoError(t, err)
	assert.Equal(t, origIndex.index, resOrigIndex)
	assert.Equal(t, origIndex.index, indexColl.GetByName(resOrigIndex.Name()))
	assert.Equal(t, []Index{origIndex.index}, indexColl.AllIndexes())

	// Check that indexColl now contains origIndex
	for _, tag := range resOrigIndex.IndexedColumnTags() {
		assert.Equal(t, []Index{resOrigIndex}, indexColl.IndexesWithTag(tag))
	}
	for _, col := range resOrigIndex.ColumnNames() {
		assert.Equal(t, []Index{resOrigIndex}, indexColl.IndexesWithColumn(col))
	}
	assert.True(t, indexColl.Contains(resOrigIndex.Name()))
	assert.True(t, indexColl.hasIndexOnColumns(resOrigIndex.ColumnNames()...))
	assert.True(t, indexColl.hasIndexOnTags(resOrigIndex.IndexedColumnTags()...))

	// Check that indexColl doesn't yet contain copyIndex by name, but by other properties
	assert.False(t, indexColl.Contains(copyIndex.index.Name()))
	assert.True(t, indexColl.hasIndexOnColumns(copyIndex.index.ColumnNames()...))
	assert.True(t, indexColl.hasIndexOnTags(copyIndex.index.IndexedColumnTags()...))
	assert.Nil(t, indexColl.GetByName(copyIndex.index.Name()))

	// Insert copyIndex and see that no errors occur
	resCopyIndex, err := indexColl.AddIndexByColNames(copyIndex.index.Name(), copyIndex.cols, IndexProperties{IsUnique: copyIndex.index.IsUnique(), Comment: copyIndex.index.Comment()})
	assert.NoError(t, err)
	assert.Equal(t, copyIndex.index, resCopyIndex)
	assert.Equal(t, copyIndex.index, indexColl.GetByName(resCopyIndex.Name()))
	assert.Equal(t, []Index{copyIndex.index, origIndex.index}, indexColl.AllIndexes())

	// Check that indexColl contains both resOrigIndex and resCopyIndex
	for _, tag := range resCopyIndex.IndexedColumnTags() {
		assert.Equal(t, []Index{resOrigIndex, resCopyIndex}, indexColl.IndexesWithTag(tag))
	}
	for _, col := range resCopyIndex.ColumnNames() {
		assert.Equal(t, []Index{resOrigIndex, resCopyIndex}, indexColl.IndexesWithColumn(col))
	}
	assert.True(t, indexColl.Contains(resCopyIndex.Name()))
	assert.True(t, indexColl.hasIndexOnColumns(resCopyIndex.ColumnNames()...))
	assert.True(t, indexColl.hasIndexOnTags(resCopyIndex.IndexedColumnTags()...))

	indexColl.clear(t)
}

func (ixc *indexCollectionImpl) clear(_ *testing.T) {
	ixc.indexes = make(map[string]*indexImpl)
	for key := range ixc.colTagToIndex {
		ixc.colTagToIndex[key] = nil
	}
}
