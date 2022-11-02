// Copyright 2019 Dolthub, Inc.
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

package edits

import (
	"context"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/types"
)

func (coll *KVPCollection) String() string {
	ctx := context.Background()

	itr := coll.Iterator()
	val, err := itr.Next()
	d.PanicIfError(err)

	keys := make([]types.Value, coll.totalSize)
	for i := 0; val != nil; i++ {
		var err error
		keys[i], err = val.Key.Value(ctx)
		d.PanicIfError(err)

		val, err = itr.Next()
		d.PanicIfError(err)
	}

	tpl, err := types.NewTuple(coll.nbf, keys...)
	d.PanicIfError(err)

	str, err := types.EncodedValue(ctx, tpl)
	d.PanicIfError(err)

	return str
}

func TestKVPCollection(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	nbf := types.Format_Default
	testKVPCollection(t, nbf, rng)

	for i := 0; i < 64; i++ {
		seed := time.Now().UnixNano()
		t.Log(seed)
		rng := rand.New(rand.NewSource(seed))
		testKVPCollection(t, nbf, rng)
	}
}

func TestKVPCollectionDestructiveMergeStable(t *testing.T) {
	left := NewKVPCollection(types.Format_Default, types.KVPSlice{
		types.KVP{Key: types.Int(0)},
		types.KVP{Key: types.Int(1)},
		types.KVP{Key: types.Int(2)},
	})
	right := NewKVPCollection(types.Format_Default, types.KVPSlice{
		types.KVP{Key: types.Int(0), Val: types.Int(0)},
		types.KVP{Key: types.Int(1), Val: types.Int(0)},
		types.KVP{Key: types.Int(2), Val: types.Int(0)},
	})
	var err error
	left, err = left.DestructiveMerge(right)
	assert.NoError(t, err)
	i := left.Iterator()
	var v *types.KVP
	v, err = i.Next()
	assert.NoError(t, err)
	assert.Equal(t, types.Int(0), v.Key)
	assert.Nil(t, v.Val)
	v, err = i.Next()
	assert.NoError(t, err)
	assert.Equal(t, types.Int(0), v.Key)
	assert.NotNil(t, v.Val)
	v, err = i.Next()
	assert.NoError(t, err)
	assert.Equal(t, types.Int(1), v.Key)
	assert.Nil(t, v.Val)
	v, err = i.Next()
	assert.NoError(t, err)
	assert.Equal(t, types.Int(1), v.Key)
	assert.NotNil(t, v.Val)
	v, err = i.Next()
	assert.NoError(t, err)
	assert.Equal(t, types.Int(2), v.Key)
	assert.Nil(t, v.Val)
	v, err = i.Next()
	assert.NoError(t, err)
	assert.Equal(t, types.Int(2), v.Key)
	assert.NotNil(t, v.Val)
	_, err = i.Next()
	assert.Equal(t, io.EOF, err)
}

func testKVPCollection(t *testing.T, nbf *types.NomsBinFormat, rng *rand.Rand) {
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
		colls[i] = createKVPColl(nbf, rng, size)
	}

	for len(colls) > 1 {
		for i, coll := range colls {
			inOrder, _, err := IsInOrder(nbf, NewItr(nbf, coll))
			assert.NoError(t, err)
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
				mergedColl, err := colls[i].DestructiveMerge(colls[j])
				assert.NoError(t, err)

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

	inOrder, numItems, err := IsInOrder(nbf, NewItr(nbf, colls[0]))
	assert.NoError(t, err)
	if !inOrder {
		t.Fatal("collection not in order")
	} else if numItems != numColls*size {
		t.Fatal("Unexpected size")
	}
}

func createKVPColl(nbf *types.NomsBinFormat, rng *rand.Rand, size int) *KVPCollection {
	kvps := make(types.KVPSlice, size)

	for i := 0; i < size; i++ {
		kvps[i] = types.KVP{Key: types.Uint(rng.Uint64() % 10000), Val: types.NullValue}
	}

	types.SortWithErroringLess(types.KVPSort{Values: kvps, NBF: nbf})

	return NewKVPCollection(nbf, kvps)
}
