package row

import (
	"context"
	"fmt"
	"github.com/attic-labs/noms/go/types"
)

type TaggedValues map[uint64]types.Value

func (tt TaggedValues) NomsTupleForTags(tags []uint64, encodeNulls bool) types.Tuple {
	numVals := len(tags)
	vals := make([]types.Value, 0, 2*numVals)

	for _, tag := range tags {
		val := tt[tag]

		if val == nil && encodeNulls {
			val = types.NullValue
		}

		if val != nil {
			vals = append(vals, types.Uint(tag), val)
		}
	}

	return types.NewTuple(vals...)
}

func (tt TaggedValues) Iter(cb func(tag uint64, val types.Value) (stop bool)) bool {
	stop := false
	for tag, val := range tt {
		stop = cb(tag, val)

		if stop {
			break
		}
	}

	return stop
}

func (tt TaggedValues) Get(tag uint64) (types.Value, bool) {
	val, ok := tt[tag]
	return val, ok
}

func (tt TaggedValues) Set(tag uint64, val types.Value) TaggedValues {
	updated := tt.copy()
	// Setting a nil value removes the mapping for that tag entirely, rather than setting a nil value. The methods to
	// write to noms treat a nil value the same as an absent value.
	if val != nil {
		updated[tag] = val
	} else {
		delete(updated, tag)
	}

	return updated
}

func (tt TaggedValues) copy() TaggedValues {
	newTagToVal := make(TaggedValues, len(tt))
	for tag, val := range tt {
		newTagToVal[tag] = val
	}

	return newTagToVal
}

func ParseTaggedValues(tpl types.Tuple) TaggedValues {
	if tpl.Len()%2 != 0 {
		panic("A tagged tuple must have an even column count.")
	}

	taggedTuple := make(TaggedValues, tpl.Len()/2)
	for i := uint64(0); i < tpl.Len(); i += 2 {
		tag := tpl.Get(i)
		val := tpl.Get(i + 1)

		if tag.Kind() != types.UintKind {
			panic("Invalid tagged tuple must have uint tags.")
		}

		if val != types.NullValue {
			taggedTuple[uint64(tag.(types.Uint))] = val
		}
	}

	return taggedTuple
}

func (tt TaggedValues) String() string {
	str := "{"
	for k, v := range tt {
		str += fmt.Sprintf("\n\t%d: %s", k, types.EncodedValue(context.Background(), v))
	}

	str += "\n}"
	return str
}
