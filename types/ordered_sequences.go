package types

import (
	"sort"

	"github.com/attic-labs/noms/chunks"
)

func isSequenceOrderedByIndexedType(t Type) bool {
	return t.Desc.(CompoundDesc).ElemTypes[0].IsOrdered()
}

// Given a leaf in an ordered sequence, returns the values in that leaf which define the ordering of the sequence.
type getLeafOrderedValuesFn func(Value) []Value

// Returns a cursor to |key| in |ms|, plus the leaf + index that |key| is in. |t| is the type of the ordered values.
func findLeafInOrderedSequence(ms metaSequence, t Type, key Value, getValues getLeafOrderedValuesFn, cs chunks.ChunkStore) (cursor *sequenceCursor, leaf Value, idx int) {
	cursor, leaf = newMetaSequenceCursor(ms, cs)

	if isSequenceOrderedByIndexedType(t) {
		orderedKey := key.(OrderedValue)

		cursor.seekBinary(func(mt sequenceItem) bool {
			return !mt.(metaTuple).value.(OrderedValue).Less(orderedKey)
		})
	} else {
		cursor.seekBinary(func(mt sequenceItem) bool {
			return !mt.(metaTuple).value.(Ref).TargetRef().Less(key.Ref())
		})
	}

	if current := cursor.current().(metaTuple); current.ref != valueFromType(cs, leaf, leaf.Type()).Ref() {
		leaf = readMetaTupleValue(cursor.current(), cs)
	}

	if leafData := getValues(leaf); isSequenceOrderedByIndexedType(t) {
		orderedKey := key.(OrderedValue)

		idx = sort.Search(len(leafData), func(i int) bool {
			return !leafData[i].(OrderedValue).Less(orderedKey)
		})
	} else {
		idx = sort.Search(len(leafData), func(i int) bool {
			return !leafData[i].Ref().Less(key.Ref())
		})
	}

	return
}
