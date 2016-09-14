// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/profile"
	flag "github.com/juju/gnuflag"
)

var (
	count    = flag.Uint64("count", 100000, "number of elements")
	blobSize = flag.Uint64("blobsize", 2<<24 /* 32MB */, "size of blob of create")
)

const numberSize = uint64(8)
const strPrefix = "i am a 32 bytes.....%12d"
const stringSize = uint64(32)
const boolSize = uint64(1)
const structSize = uint64(64)

func main() {
	profile.RegisterProfileFlags(flag.CommandLine)
	flag.Parse(true)

	buildCount := *count
	insertCount := buildCount / 50
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
			ms := chunks.NewMemoryStore()
			ds := dataset.NewDataset(datas.NewDatabase(ms), "test")
			t1 := time.Now()
			col := buildFns[i](buildCount, valueFn)
			ds, err := ds.CommitValue(col)
			d.Chk.NoError(err)
			buildDuration := time.Since(t1)

			// Read
			t1 = time.Now()
			col = ds.HeadValue().(types.Collection)
			readFns[i](col)
			readDuration := time.Since(t1)

			// Build Incrementally
			ms = chunks.NewMemoryStore()
			ds = dataset.NewDataset(datas.NewDatabase(ms), "test")
			t1 = time.Now()
			col = buildIncrFns[i](insertCount, valueFn)
			ds, err = ds.CommitValue(col)
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

	ms := chunks.NewMemoryStore()
	ds := dataset.NewDataset(datas.NewDatabase(ms), "test")

	blobBytes := makeBlobBytes(*blobSize)
	t1 := time.Now()
	blob := types.NewBlob(bytes.NewReader(blobBytes))
	ds.CommitValue(blob)
	buildDuration := time.Since(t1)

	ds = dataset.NewDataset(datas.NewDatabase(ms), "test")
	t1 = time.Now()
	blob = ds.HeadValue().(types.Blob)
	outBytes, _ := ioutil.ReadAll(blob.Reader())
	readDuration := time.Since(t1)
	d.PanicIfFalse(bytes.Compare(blobBytes, outBytes) == 0)
	fmt.Printf("\t\t\t%s\t\t%s\n\n", rate(buildDuration, *blobSize), rate(readDuration, *blobSize))
}

func rate(d time.Duration, size uint64) string {
	return fmt.Sprintf("%d ms (%.2f MB/s)", uint64(d)/1000000, float64(size)*1000/float64(d))
}

type createValueFn func(i uint64) types.Value
type buildCollectionFn func(count uint64, createFn createValueFn) types.Collection
type readCollectionFn func(value types.Collection)

func makeBlobBytes(byteLength uint64) []byte {
	buff := &bytes.Buffer{}
	counter := uint64(0)
	for uint64(buff.Len()) < byteLength {
		binary.Write(buff, binary.BigEndian, counter)
		counter++
	}
	return buff.Bytes()
}

func createString(i uint64) types.Value {
	return types.String(fmt.Sprintf("%s%d", strPrefix, i))
}

func createNumber(i uint64) types.Value {
	return types.Number(i)
}

var structType = types.MakeStructType("S1",
	[]string{"bool", "num", "str"},
	[]*types.Type{
		types.BoolType,
		types.NumberType,
		types.StringType,
	})

func createStruct(i uint64) types.Value {
	return types.NewStructWithType(structType, types.ValueSlice{
		types.Bool(i%2 == 0),
		types.Number(i),
		types.String(fmt.Sprintf("i am a 55 bytes............................%12d", i)),
	})
}

func buildList(count uint64, createFn createValueFn) types.Collection {
	values := make([]types.Value, count)
	for i := uint64(0); i < count; i++ {
		values[i] = createFn(i)
	}

	return types.NewList(values...)
}

func buildListIncrementally(count uint64, createFn createValueFn) types.Collection {
	l := types.NewList()
	for i := uint64(0); i < count; i++ {
		l = l.Insert(i, createFn(i))
	}

	return l
}

func readList(c types.Collection) {
	c.(types.List).IterAll(func(v types.Value, idx uint64) {
	})
}

func buildSet(count uint64, createFn createValueFn) types.Collection {
	values := make([]types.Value, count)
	for i := uint64(0); i < count; i++ {
		values[i] = createFn(i)
	}

	return types.NewSet(values...)
}

func buildSetIncrementally(count uint64, createFn createValueFn) types.Collection {
	s := types.NewSet()
	for i := uint64(0); i < count; i++ {
		s = s.Insert(createFn(i))
	}

	return s
}

func readSet(c types.Collection) {
	c.(types.Set).IterAll(func(v types.Value) {
	})
}

func buildMap(count uint64, createFn createValueFn) types.Collection {
	values := make([]types.Value, count*2)
	for i := uint64(0); i < count*2; i++ {
		values[i] = createFn(i)
	}

	return types.NewMap(values...)
}

func buildMapIncrementally(count uint64, createFn createValueFn) types.Collection {
	m := types.NewMap()

	for i := uint64(0); i < count*2; i += 2 {
		m = m.Set(createFn(i), createFn(i+1))
	}

	return m
}

func readMap(c types.Collection) {
	c.(types.Map).IterAll(func(k types.Value, v types.Value) {
	})
}
