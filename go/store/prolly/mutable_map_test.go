package prolly

import (
	"testing"

	"github.com/dolthub/dolt/go/store/val"
)

func TestMutableMapReads(t *testing.T) {
	//t.Run("get item from map", func(t *testing.T) {
	//	testOrderedMapGetAndHas(t, makeMutableMap, 10)
	//	testOrderedMapGetAndHas(t, makeMutableMap, 100)
	//	testOrderedMapGetAndHas(t, makeMutableMap, 1000)
	//	testOrderedMapGetAndHas(t, makeMutableMap, 10_000)
	//})
	//t.Run("get from map at index", func(t *testing.T) {
	//	testOrderedMapGetIndex(t, makeMutableMap, 10)
	//	testOrderedMapGetIndex(t, makeMutableMap, 100)
	//	testOrderedMapGetIndex(t, makeMutableMap, 1000)
	//	testOrderedMapGetIndex(t, makeMutableMap, 10_000)
	//})
	//t.Run("get value range from map", func(t *testing.T) {
	//	testMapIterValueRange(t, 10)
	//	testMapIterValueRange(t, 100)
	//	testMapIterValueRange(t, 1000)
	//	testMapIterValueRange(t, 10_000)
	//})
	//t.Run("get index range from map", func(t *testing.T) {
	//	testOrderedMapIterIndexRange(t, makeMutableMap, 10)
	//	testOrderedMapIterIndexRange(t, makeMutableMap, 100)
	//	testOrderedMapIterIndexRange(t, makeMutableMap, 1000)
	//	testOrderedMapIterIndexRange(t, makeMutableMap, 10_000)
	//})
}

func makeMutableMap(t *testing.T, kd, vd val.TupleDesc, items [][2]val.Tuple) orderedMap {
	m := makeProllyMap(t, kd, vd, items)
	return m.(Map).Mutate()
}

var _ cartographer = makeMutableMap
