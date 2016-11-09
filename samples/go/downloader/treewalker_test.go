// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

type Point struct {
	X int
	Y int
}

type TStruct struct {
	M1 map[string]int
	M2 map[Point]string
	L1 []Point
	L2 []string
}

func mustMarshal(v interface{}) types.Value {
	v1, _ := marshal.Marshal(v)
	return v1
}

func TestTreeWalk(t *testing.T) {
	assert := assert.New(t)
	store := types.NewTestValueStore()

	m1 := map[string]int{"map1-key1": 1, "map1-key2": 2}
	m2 := map[Point]string{{100, 200}: "map2-val1", {300, 400}: "map2-val2"}
	l1 := []Point{{11, 12}, {13, 14}}
	l2 := []string{"list2-val1", "list2-val2"}
	ts := TStruct{M1: m1, M2: m2, L1: l1, L2: l2}
	tv := mustMarshal(ts).(types.Struct)

	nomsL1 := mustMarshal(l1).(types.List)
	nomsL2 := mustMarshal(l2).(types.List)
	nomsM1 := mustMarshal(m1).(types.Map)
	nomsM2 := mustMarshal(m2).(types.Map)
	nomsP1 := mustMarshal(Point{100, 200})
	nomsP2 := mustMarshal(Point{300, 400})

	nomsR1 := store.WriteValue(nomsP1)
	nomsS1 := types.NewSet(types.String("one"), types.Number(2), nomsP1)
	tv = tv.Set("r1", nomsR1).Set("s1", nomsS1)
	store.WriteValue(tv)

	expected := map[string][]types.Value{
		`.l1`:      {tv, nomsL1},
		`.l1[0]`:   {nomsL1, nomsL1.Get(0)},
		`.l1[0].x`: {nomsL1.Get(0), types.Number(11)},
		`.l1[0].y`: {nomsL1.Get(0), types.Number(12)},
		`.l1[1]`:   {nomsL1, nomsL1.Get(1)},
		`.l1[1].x`: {nomsL1.Get(1), types.Number(13)},
		`.l1[1].y`: {nomsL1.Get(1), types.Number(14)},
		`.l2`:      {tv, nomsL2},
		`.l2[0]`:   {nomsL2, nomsL2.Get(0)},
		`.l2[1]`:   {nomsL2, nomsL2.Get(1)},
		`.m1`:      {tv, nomsM1},
		`.m1["map1-key1"]@key`: {nomsM1, types.String("map1-key1")},
		`.m1["map1-key1"]`:     {nomsM1, types.Number(1)},
		`.m1["map1-key2"]@key`: {nomsM1, types.String("map1-key2")},
		`.m1["map1-key2"]`:     {nomsM1, types.Number(2)},
		`.m2`:                  {tv, nomsM2},
		fmt.Sprintf(`.m2[#%s]@key`, nomsP1.Hash()):   {nomsM2, nomsP1},
		fmt.Sprintf(`.m2[#%s]@key.x`, nomsP1.Hash()): {nomsP1, types.Number(100)},
		fmt.Sprintf(`.m2[#%s]@key.y`, nomsP1.Hash()): {nomsP1, types.Number(200)},
		fmt.Sprintf(`.m2[#%s]`, nomsP1.Hash()):       {nomsM2, types.String("map2-val1")},
		fmt.Sprintf(`.m2[#%s]@key`, nomsP2.Hash()):   {nomsM2, nomsP2},
		fmt.Sprintf(`.m2[#%s]@key.x`, nomsP2.Hash()): {nomsP2, types.Number(300)},
		fmt.Sprintf(`.m2[#%s]@key.y`, nomsP2.Hash()): {nomsP2, types.Number(400)},
		fmt.Sprintf(`.m2[#%s]`, nomsP2.Hash()):       {nomsM2, types.String("map2-val2")},
		`.s1`: {tv, nomsS1},
		fmt.Sprintf(`.s1[#%s]`, types.String("one").Hash()): {nomsS1, types.String("one")},
		fmt.Sprintf(`.s1[#%s]`, types.Number(2).Hash()):     {nomsS1, types.Number(2)},
		fmt.Sprintf(`.s1[#%s]`, nomsP1.Hash()):              {nomsS1, nomsP1},
		fmt.Sprintf(`.s1[#%s].x`, nomsP1.Hash()):            {nomsP1, types.Number(100)},
		fmt.Sprintf(`.s1[#%s].y`, nomsP1.Hash()):            {nomsP1, types.Number(200)},
		`.r1`:   {tv, nomsP1},
		`.r1.x`: {nomsP1, types.Number(100)},
		`.r1.y`: {nomsP1, types.Number(200)},
	}

	cnt := 0
	callback := func(p types.Path, parent, value types.Value) bool {
		if vs, ok := expected[p.String()]; ok {
			cnt += 1
			assert.True(parent.Equals(vs[0]), "p: %s, parent: %s, value: %s", p, types.EncodedValue(parent), types.EncodedValue(value))
			assert.True(value.Equals(vs[1]), "p: %s, parent: %s, value: %s", p, types.EncodedValue(parent), types.EncodedValue(value))
		}
		return false
	}

	TreeWalk(store, nil, tv, callback)
	assert.Equal(len(expected), cnt)
}
