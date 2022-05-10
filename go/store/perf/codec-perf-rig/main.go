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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	flag "github.com/juju/gnuflag"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/profile"
)

var (
	count    = flag.Uint64("count", 100000, "number of elements")
	blobSize = flag.Uint64("blobsize", 2<<24 /* 32MB */, "size of blob of create")
)

const numberSize = uint64(8)
const strPrefix = "i am a 32 bytes.....%12d"
const stringSize = uint64(32)
const structSize = uint64(64)

func main() {
	profile.RegisterProfileFlags(flag.CommandLine)
	flag.Parse(true)

	buildCount := *count
	insertCount := buildCount
	defer profile.MaybeStartProfile().Stop()

	collectionTypes := []string{"List", "Set", "Map"}
	buildFns := []buildCollectionFn{buildList, buildSet, buildMap}
	buildIncrFns := []buildCollectionFn{buildListIncrementally, buildSetIncrementally, buildMapIncrementally}
	readFns := []readCollectionFn{readList, readSet, readMap}

	elementTypes := []string{"numbers (8 B)", "strings (32 B)", "structs (64 B)"}
	elementSizes := []uint64{numberSize, stringSize, structSize}
	valueFns := []createValueFn{createNumber, createString, createStruct}

	for i, colType := range collectionTypes {
		fmt.Printf("Testing %s: \t\tbuild %d\t\t\tscan %d\t\t\tinsert %d\n", colType, buildCount, buildCount, insertCount)

		for j, elementType := range elementTypes {
			valueFn := valueFns[j]

			// Build One-Time
			storage := &chunks.MemoryStorage{}
			vrw := types.NewValueStore(storage.NewViewWithDefaultFormat())
			db := datas.NewTypesDatabase(vrw)
			ds, err := db.GetDataset(context.Background(), "test")
			d.Chk.NoError(err)
			t1 := time.Now()
			col := buildFns[i](vrw, buildCount, valueFn)
			ds, err = datas.CommitValue(context.Background(), db, ds, col)
			d.Chk.NoError(err)
			buildDuration := time.Since(t1)

			// Read
			t1 = time.Now()
			val, ok, err := ds.MaybeHeadValue()
			d.Chk.NoError(err)
			d.Chk.True(ok)
			col = val.(types.Collection)
			readFns[i](col)
			readDuration := time.Since(t1)

			// Build Incrementally
			storage = &chunks.MemoryStorage{}
			vrw = types.NewValueStore(storage.NewViewWithDefaultFormat())
			db = datas.NewTypesDatabase(vrw)
			ds, err = db.GetDataset(context.Background(), "test")
			d.Chk.NoError(err)
			t1 = time.Now()
			col = buildIncrFns[i](vrw, insertCount, valueFn)
			ds, err = datas.CommitValue(context.Background(), db, ds, col)
			d.Chk.NoError(err)
			incrDuration := time.Since(t1)

			elementSize := elementSizes[j]
			buildSize := elementSize * buildCount
			incrSize := elementSize * insertCount

			fmt.Printf("%s\t\t%s\t\t%s\t\t%s\n", elementType, rate(buildDuration, buildSize), rate(readDuration, buildSize), rate(incrDuration, incrSize))
		}
		fmt.Println()
	}

	fmt.Printf("Testing Blob: \t\tbuild %d MB\t\t\tscan %d MB\n", *blobSize/1000000, *blobSize/1000000)

	storage := &chunks.MemoryStorage{}
	vrw := types.NewValueStore(storage.NewViewWithDefaultFormat())
	db := datas.NewTypesDatabase(vrw)
	ds, err := db.GetDataset(context.Background(), "test")
	d.Chk.NoError(err)

	blobBytes := makeBlobBytes(*blobSize)
	t1 := time.Now()
	blob, err := types.NewBlob(context.Background(), vrw, bytes.NewReader(blobBytes))
	d.Chk.NoError(err)
	_, err = datas.CommitValue(context.Background(), db, ds, blob)
	d.Chk.NoError(err)
	buildDuration := time.Since(t1)

	db = datas.NewDatabase(storage.NewViewWithDefaultFormat())
	ds, err = db.GetDataset(context.Background(), "test")
	d.Chk.NoError(err)
	t1 = time.Now()
	blobVal, ok, err := ds.MaybeHeadValue()
	d.Chk.NoError(err)
	d.Chk.True(ok)
	blob = blobVal.(types.Blob)
	buff := &bytes.Buffer{}
	blob.Copy(context.Background(), buff)
	outBytes := buff.Bytes()
	readDuration := time.Since(t1)
	d.PanicIfFalse(bytes.Equal(blobBytes, outBytes))
	fmt.Printf("\t\t\t%s\t\t%s\n\n", rate(buildDuration, *blobSize), rate(readDuration, *blobSize))
}

func rate(d time.Duration, size uint64) string {
	return fmt.Sprintf("%d ms (%.2f MB/s)", uint64(d)/1000000, float64(size)*1000/float64(d))
}

type createValueFn func(i uint64) types.Value
type buildCollectionFn func(vrw types.ValueReadWriter, count uint64, createFn createValueFn) types.Collection
type readCollectionFn func(value types.Collection)

func makeBlobBytes(byteLength uint64) []byte {
	buff := &bytes.Buffer{}
	counter := uint64(0)
	for uint64(buff.Len()) < byteLength {
		err := binary.Write(buff, binary.BigEndian, counter)
		d.Chk.NoError(err)
		counter++
	}
	return buff.Bytes()
}

func createString(i uint64) types.Value {
	return types.String(fmt.Sprintf("%s%d", strPrefix, i))
}

func createNumber(i uint64) types.Value {
	return types.Float(i)
}

var structTemplate = types.MakeStructTemplate("S1", []string{"bool", "num", "str"})

func createStruct(i uint64) types.Value {
	st, err := structTemplate.NewStruct(types.Format_Default, []types.Value{
		types.Bool(i%2 == 0), // "bool"
		types.Float(i),       // "num"
		types.String(fmt.Sprintf("i am a 55 bytes............................%12d", i)), // "str"
	})

	d.Chk.NoError(err)

	return st
}

func buildList(vrw types.ValueReadWriter, count uint64, createFn createValueFn) types.Collection {
	values := make([]types.Value, count)
	for i := uint64(0); i < count; i++ {
		values[i] = createFn(i)
	}

	l, err := types.NewList(context.Background(), vrw, values...)

	d.Chk.NoError(err)

	return l
}

func buildListIncrementally(vrw types.ValueReadWriter, count uint64, createFn createValueFn) types.Collection {
	l, err := types.NewList(context.Background(), vrw)

	d.Chk.NoError(err)

	le := l.Edit()

	for i := uint64(0); i < count; i++ {
		le.Append(createFn(i))
	}

	l, err = le.List(context.Background())

	d.Chk.NoError(err)

	return l
}

func readList(c types.Collection) {
	_ = c.(types.List).IterAll(context.Background(), func(v types.Value, idx uint64) error {
		return nil
	})
}

func buildSet(vrw types.ValueReadWriter, count uint64, createFn createValueFn) types.Collection {
	values := make([]types.Value, count)
	for i := uint64(0); i < count; i++ {
		values[i] = createFn(i)
	}

	s, err := types.NewSet(context.Background(), vrw, values...)

	d.Chk.NoError(err)

	return s
}

func buildSetIncrementally(vrw types.ValueReadWriter, count uint64, createFn createValueFn) types.Collection {
	s, err := types.NewSet(context.Background(), vrw)
	d.Chk.NoError(err)

	se := s.Edit()
	for i := uint64(0); i < count; i++ {
		se.Insert(createFn(i))
	}

	s, err = se.Set(context.Background())
	d.Chk.NoError(err)

	return s
}

func readSet(c types.Collection) {
	_ = c.(types.Set).IterAll(context.Background(), func(v types.Value) error {
		return nil
	})
}

func buildMap(vrw types.ValueReadWriter, count uint64, createFn createValueFn) types.Collection {
	values := make([]types.Value, count*2)
	for i := uint64(0); i < count*2; i++ {
		values[i] = createFn(i)
	}

	m, err := types.NewMap(context.Background(), vrw, values...)

	d.Chk.NoError(err)

	return m
}

func buildMapIncrementally(vrw types.ValueReadWriter, count uint64, createFn createValueFn) types.Collection {
	m, err := types.NewMap(context.Background(), vrw)
	d.Chk.NoError(err)

	me := m.Edit()

	for i := uint64(0); i < count*2; i += 2 {
		me.Set(createFn(i), createFn(i+1))
	}

	m, err = me.Map(context.Background())
	d.Chk.NoError(err)

	return m
}

func readMap(c types.Collection) {
	_ = c.(types.Map).IterAll(context.Background(), func(k types.Value, v types.Value) error {
		return nil
	})
}
