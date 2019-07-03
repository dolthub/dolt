package edits

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func (coll *KVPCollection) String() string {
	ctx := context.Background()

	itr := coll.Iterator()
	val := itr.Next()

	keys := make([]types.Value, coll.totalSize)
	for i := 0; val != nil; i++ {
		keys[i] = val.Key.Value(ctx)
		val = itr.Next()
	}

	tpl := types.NewTuple(types.Format_7_18, keys...)
	return types.EncodedValue(ctx, tpl)
}

func TestKVPCollection(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	testKVPCollection(t, rng)

	for i := 0; i < 64; i++ {
		seed := time.Now().UnixNano()
		t.Log(seed)
		rng := rand.New(rand.NewSource(seed))
		testKVPCollection(t, rng)
	}
}

func testKVPCollection(t *testing.T, rng *rand.Rand) {
	const (
		maxSize = 1024
		minSize = 4

		maxColls = 128
		minColls = 3
	)

	numColls := int(minColls + rng.Int31n(maxColls-minColls))
	colls := make([]*KVPCollection, numColls)
	size := int(minSize + rng.Int31n(maxSize-minSize))

	t.Log("num collections:", numColls, "- buffer size", size)

	for i := 0; i < numColls; i++ {
		colls[i] = createKVPColl(rng, size)
	}

	for len(colls) > 1 {
		for i, coll := range colls {
			inOrder, _ := IsInOrder(NewItr(types.Format_7_18, coll))
			if !inOrder {
				t.Fatal(i, "not in order")
			}
		}

		var newColls []*KVPCollection
		for i, j := 0, len(colls)-1; i <= j; i, j = i+1, j-1 {
			if i == j {
				newColls = append(newColls, colls[i])
			} else {
				s1 := colls[i].Size()
				s2 := colls[j].Size()
				//fmt.Print(colls[i].String(), "+", colls[j].String())
				mergedColl := colls[i].DestructiveMerge(colls[j])

				ms := mergedColl.Size()

				if s1+s2 != ms {
					t.Fatal("wrong size")
				}

				//fmt.Println("=", mergedColl.String())
				newColls = append(newColls, mergedColl)
			}
		}

		colls = newColls
	}

	inOrder, numItems := IsInOrder(NewItr(types.Format_7_18, colls[0]))
	if !inOrder {
		t.Fatal("collection not in order")
	} else if numItems != numColls*size {
		t.Fatal("Unexpected size")
	}
}

func createKVPColl(rng *rand.Rand, size int) *KVPCollection {
	kvps := make(types.KVPSlice, size)

	for i := 0; i < size; i++ {
		kvps[i] = types.KVP{types.Uint(rng.Uint64() % 10000), types.NullValue}
	}

	sort.Stable(types.KVPSort{kvps, types.Format_7_18})

	return NewKVPCollection(types.Format_7_18, kvps)
}
