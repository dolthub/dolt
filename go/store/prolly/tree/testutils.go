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

package tree

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

var testRand = rand.New(rand.NewSource(1))

func NewTupleLeafNode(keys, values []val.Tuple) Node {
	ks := make([]Item, len(keys))
	for i := range ks {
		ks[i] = Item(keys[i])
	}
	vs := make([]Item, len(values))
	for i := range vs {
		vs[i] = Item(values[i])
	}
	return newLeafNode(ks, vs)
}

func RandomTuplePairs(ctx context.Context, count int, keyDesc, valDesc val.TupleDesc, ns NodeStore) (items [][2]val.Tuple) {
	keyBuilder := val.NewTupleBuilder(keyDesc)
	valBuilder := val.NewTupleBuilder(valDesc)

	items = make([][2]val.Tuple, count)
	for i := range items {
		items[i][0] = RandomTuple(keyBuilder, ns)
		items[i][1] = RandomTuple(valBuilder, ns)
	}

	dupes := make([]int, 0, count)
	for {
		SortTuplePairs(ctx, items, keyDesc)
		for i := range items {
			if i == 0 {
				continue
			}
			if keyDesc.Compare(ctx, items[i][0], items[i-1][0]) == 0 {
				dupes = append(dupes, i)
			}
		}
		if len(dupes) == 0 {
			break
		}

		// replace duplicates and validate again
		for _, d := range dupes {
			items[d][0] = RandomTuple(keyBuilder, ns)
		}
		dupes = dupes[:0]
	}
	return items
}

func RandomCompositeTuplePairs(ctx context.Context, count int, keyDesc, valDesc val.TupleDesc, ns NodeStore) (items [][2]val.Tuple) {
	// preconditions
	if count%5 != 0 {
		panic("expected empty divisible by 5")
	}
	if len(keyDesc.Types) < 2 {
		panic("expected composite key")
	}

	tt := RandomTuplePairs(ctx, count, keyDesc, valDesc, ns)

	tuples := make([][2]val.Tuple, len(tt)*3)
	for i := range tuples {
		j := i % len(tt)
		tuples[i] = tt[j]
	}

	// permute the second column
	swap := make([]byte, len(tuples[0][0].GetField(1)))
	rand.Shuffle(len(tuples), func(i, j int) {
		f1 := tuples[i][0].GetField(1)
		f2 := tuples[i][0].GetField(1)
		copy(swap, f1)
		copy(f1, f2)
		copy(f2, swap)
	})

	SortTuplePairs(ctx, tuples, keyDesc)

	tuples = deduplicateTuples(ctx, keyDesc, tuples)

	return tuples[:count]
}

// Map<Tuple<Uint32>,Tuple<Uint32>>
func AscendingUintTuples(count int) (tuples [][2]val.Tuple, desc val.TupleDesc) {
	desc = val.NewTupleDescriptor(val.Type{Enc: val.Uint32Enc})
	bld := val.NewTupleBuilder(desc)
	tuples = make([][2]val.Tuple, count)
	for i := range tuples {
		bld.PutUint32(0, uint32(i))
		tuples[i][0] = bld.Build(sharedPool)
		bld.PutUint32(0, uint32(i+count))
		tuples[i][1] = bld.Build(sharedPool)
	}
	return
}

func RandomTuple(tb *val.TupleBuilder, ns NodeStore) (tup val.Tuple) {
	for i, typ := range tb.Desc.Types {
		randomField(tb, i, typ, ns)
	}
	return tb.Build(sharedPool)
}

func CloneRandomTuples(items [][2]val.Tuple) (clone [][2]val.Tuple) {
	clone = make([][2]val.Tuple, len(items))
	for i := range clone {
		clone[i] = items[i]
	}
	return
}

func SortTuplePairs(ctx context.Context, items [][2]val.Tuple, keyDesc val.TupleDesc) {
	sort.Slice(items, func(i, j int) bool {
		return keyDesc.Compare(ctx, items[i][0], items[j][0]) < 0
	})
}

func ShuffleTuplePairs(items [][2]val.Tuple) {
	testRand.Shuffle(len(items), func(i, j int) {
		items[i], items[j] = items[j], items[i]
	})
}

func NewEmptyTestNode() Node {
	return newLeafNode(nil, nil)
}

func newLeafNode(keys, values []Item) Node {
	kk := make([][]byte, len(keys))
	for i := range keys {
		kk[i] = keys[i]
	}
	vv := make([][]byte, len(values))
	for i := range vv {
		vv[i] = values[i]
	}

	s := message.NewProllyMapSerializer(val.TupleDesc{}, sharedPool)
	msg := s.Serialize(kk, vv, nil, 0)
	n, _, err := NodeFromBytes(msg)
	if err != nil {
		panic(err)
	}
	return n
}

// assumes a sorted list
func deduplicateTuples(ctx context.Context, desc val.TupleDesc, tups [][2]val.Tuple) (uniq [][2]val.Tuple) {
	uniq = make([][2]val.Tuple, 1, len(tups))
	uniq[0] = tups[0]

	for i := 1; i < len(tups); i++ {
		cmp := desc.Compare(ctx, tups[i-1][0], tups[i][0])
		if cmp < 0 {
			uniq = append(uniq, tups[i])
		}
	}
	return
}

func randomField(tb *val.TupleBuilder, idx int, typ val.Type, ns NodeStore) {
	// todo(andy): add NULLs

	neg := -1
	if testRand.Int()%2 == 1 {
		neg = 1
	}

	switch typ.Enc {
	case val.Int8Enc:
		v := int8(testRand.Intn(math.MaxInt8) * neg)
		tb.PutInt8(idx, v)
	case val.Uint8Enc:
		v := uint8(testRand.Intn(math.MaxUint8))
		tb.PutUint8(idx, v)
	case val.Int16Enc:
		v := int16(testRand.Intn(math.MaxInt16) * neg)
		tb.PutInt16(idx, v)
	case val.Uint16Enc:
		v := uint16(testRand.Intn(math.MaxUint16))
		tb.PutUint16(idx, v)
	case val.Int32Enc:
		v := testRand.Int31() * int32(neg)
		tb.PutInt32(idx, v)
	case val.Uint32Enc:
		v := testRand.Uint32()
		tb.PutUint32(idx, v)
	case val.Int64Enc:
		v := testRand.Int63() * int64(neg)
		tb.PutInt64(idx, v)
	case val.Uint64Enc:
		v := testRand.Uint64()
		tb.PutUint64(idx, v)
	case val.Float32Enc:
		tb.PutFloat32(idx, testRand.Float32())
	case val.Float64Enc:
		tb.PutFloat64(idx, testRand.Float64())
	case val.StringEnc:
		buf := make([]byte, (testRand.Int63()%40)+10)
		testRand.Read(buf)
		tb.PutString(idx, string(buf))
	case val.ByteStringEnc:
		buf := make([]byte, (testRand.Int63()%40)+10)
		testRand.Read(buf)
		tb.PutByteString(idx, buf)
	case val.Hash128Enc:
		buf := make([]byte, 16)
		testRand.Read(buf)
		tb.PutHash128(idx, buf)
	case val.CommitAddrEnc:
		buf := make([]byte, 20)
		testRand.Read(buf)
		tb.PutCommitAddr(idx, hash.New(buf))
	case val.BytesAddrEnc, val.StringAddrEnc, val.JSONAddrEnc:
		len := (testRand.Int63() % 40) + 10
		buf := make([]byte, len)
		testRand.Read(buf)
		bb := ns.BlobBuilder()
		bb.Init(int(len))
		_, addr, err := bb.Chunk(context.Background(), bytes.NewReader(buf))
		if err != nil {
			panic("failed to write bytes tree")
		}
		tb.PutBytesAddr(idx, addr)
	default:
		panic("unknown encoding")
	}
}

func NewTestNodeStore() NodeStore {
	ts := &chunks.TestStorage{}
	ns := NewNodeStore(ts.NewViewWithFormat(types.Format_DOLT.VersionString()))
	bb := &blobBuilderPool
	return nodeStoreValidator{ns: ns, bbp: bb}
}

type nodeStoreValidator struct {
	ns  NodeStore
	bbp *sync.Pool
}

func (v nodeStoreValidator) ReadBytes(ctx context.Context, h hash.Hash) ([]byte, error) {
	panic("not implemented")
}

func (v nodeStoreValidator) WriteBytes(ctx context.Context, val []byte) (hash.Hash, error) {
	panic("not implemented")
}

func (v nodeStoreValidator) Read(ctx context.Context, ref hash.Hash) (Node, error) {
	nd, err := v.ns.Read(ctx, ref)
	if err != nil {
		return Node{}, err
	}

	actual := hash.Of(nd.msg)
	if ref != actual {
		err = fmt.Errorf("incorrect node hash (%s != %s)", ref, actual)
		return Node{}, err
	}
	return nd, nil
}

func (v nodeStoreValidator) ReadMany(ctx context.Context, refs hash.HashSlice) ([]Node, error) {
	nodes, err := v.ns.ReadMany(ctx, refs)
	if err != nil {
		return nil, err
	}
	for i := range nodes {
		actual := hash.Of(nodes[i].msg)
		if refs[i] != actual {
			err = fmt.Errorf("incorrect node hash (%s != %s)", refs[i], actual)
			return nil, err
		}
	}
	return nodes, nil
}

func (v nodeStoreValidator) Write(ctx context.Context, nd Node) (hash.Hash, error) {
	h, err := v.ns.Write(ctx, nd)
	if err != nil {
		return hash.Hash{}, err
	}

	actual := hash.Of(nd.msg)
	if h != actual {
		err = fmt.Errorf("incorrect node hash (%s != %s)", h, actual)
		return hash.Hash{}, err
	}
	return h, nil
}

func (v nodeStoreValidator) Pool() pool.BuffPool {
	return v.ns.Pool()
}

func (v nodeStoreValidator) BlobBuilder() *BlobBuilder {
	bb := v.bbp.Get().(*BlobBuilder)
	if bb.ns == nil {
		bb.SetNodeStore(v)
	}
	return bb
}

// PutBlobBuilder implements NodeStore.
func (v nodeStoreValidator) PutBlobBuilder(bb *BlobBuilder) {
	bb.Reset()
	v.bbp.Put(bb)
}

func (v nodeStoreValidator) PurgeCaches() {
	v.ns.PurgeCaches()
}

func (v nodeStoreValidator) Format() *types.NomsBinFormat {
	return v.ns.Format()
}
