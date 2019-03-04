package schema

import (
	"errors"
	"sort"
)

var ErrColTagCollision = errors.New("two different columns with the same tag.")

var EmptyColColl, _ = NewColCollection()

type ColCollection struct {
	cols       []Column
	Tags       []uint64
	SortedTags []uint64
	TagToCol   map[uint64]Column
	NameToCol  map[string]Column
}

func NewColCollection(cols ...Column) (*ColCollection, error) {
	var tags []uint64
	var sortedTags []uint64

	tagToCol := make(map[uint64]Column, len(cols))
	nameToCol := make(map[string]Column, len(cols))

	var uniqueCols []Column
	for i, col := range cols {
		if val, ok := tagToCol[col.Tag]; !ok {
			uniqueCols = append(uniqueCols, col)
			tagToCol[col.Tag] = col
			tags = append(tags, col.Tag)
			sortedTags = append(sortedTags, col.Tag)
			nameToCol[col.Name] = cols[i]
		} else if !val.Equals(col) {
			return nil, ErrColTagCollision
		}

	}

	sort.Slice(sortedTags, func(i, j int) bool { return sortedTags[i] < sortedTags[j] })

	return &ColCollection{uniqueCols, tags, sortedTags, tagToCol, nameToCol}, nil
}

func (cc *ColCollection) AppendColl(colColl *ColCollection) (*ColCollection, error) {
	return cc.Append(colColl.cols...)
}

func (cc *ColCollection) Append(cols ...Column) (*ColCollection, error) {
	allCols := make([]Column, 0, len(cols)+len(cc.cols))
	allCols = append(allCols, cols...)
	allCols = append(allCols, cc.cols...)

	return NewColCollection(allCols...)
}

func (cc *ColCollection) ItrUnsorted(cb func(tag uint64, col Column) (stop bool)) {
	for _, col := range cc.cols {
		stop := cb(col.Tag, col)

		if stop {
			break
		}
	}
}

func (cc *ColCollection) ItrInSortedOrder(cb func(tag uint64, col Column) (stop bool)) {
	for _, tag := range cc.SortedTags {
		val := cc.TagToCol[tag]
		stop := cb(tag, val)

		if stop {
			break
		}
	}
}

func (cc *ColCollection) GetByName(name string) (Column, bool) {
	val, ok := cc.NameToCol[name]

	if ok {
		return val, true
	}

	return InvalidCol, false
}

func (cc *ColCollection) GetByTag(tag uint64) (Column, bool) {
	val, ok := cc.TagToCol[tag]

	if ok {
		return val, true
	}

	return InvalidCol, false
}

func (cc *ColCollection) GetByUnsortedIndex(idx int) Column {
	return cc.cols[idx]
}

func (cc *ColCollection) Size() int {
	return len(cc.cols)
}
