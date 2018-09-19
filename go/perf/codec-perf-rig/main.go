// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
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
			db := datas.NewDatabase(storage.NewView())
			ds := db.GetDataset("test")
			t1 := time.Now()
			col := buildFns[i](db, buildCount, valueFn)
			ds, err := db.CommitValue(ds, col)
			d.Chk.NoError(err)
			buildDuration := time.Since(t1)

			// Read
			t1 = time.Now()
			col = ds.HeadValue().(types.Collection)
			readFns[i](col)
			readDuration := time.Since(t1)

			// Build Incrementally
			storage = &chunks.MemoryStorage{}
			db = datas.NewDatabase(storage.NewView())
			ds = db.GetDataset("test")
			t1 = time.Now()
			col = buildIncrFns[i](db, insertCount, valueFn)
			ds, err = db.CommitValue(ds, col)
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
	db := datas.NewDatabase(storage.NewView())
	ds := db.GetDataset("test")

	blobBytes := makeBlobBytes(*blobSize)
	t1 := time.Now()
	blob := types.NewBlob(db, bytes.NewReader(blobBytes))
	db.CommitValue(ds, blob)
	buildDuration := time.Since(t1)

	db = datas.NewDatabase(storage.NewView())
	ds = db.GetDataset("test")
	t1 = time.Now()
	blob = ds.HeadValue().(types.Blob)
	buff := &bytes.Buffer{}
	blob.Copy(buff)
	outBytes := buff.Bytes()
	readDuration := time.Since(t1)
	d.PanicIfFalse(bytes.Compare(blobBytes, outBytes) == 0)
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
		binary.Write(buff, binary.BigEndian, counter)
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

var structType = types.MakeStructType("S1",
	types.StructField{
		Name: "bool",
		Type: types.BoolType,
	},
	types.StructField{
		Name: "num",
		Type: types.FloaTType,
	},
	types.StructField{
		Name: "str",
		Type: types.StringType,
	},
)

var structTemplate = types.MakeStructTemplate("S1", []string{"bool", "num", "str"})

func createStruct(i uint64) types.Value {
	return structTemplate.NewStruct([]types.Value{
		types.Bool(i%2 == 0), // "bool"
		types.Float(i),       // "num"
		types.String(fmt.Sprintf("i am a 55 bytes............................%12d", i)), // "str"
	})
}

func buildList(vrw types.ValueReadWriter, count uint64, createFn createValueFn) types.Collection {
	values := make([]types.Value, count)
	for i := uint64(0); i < count; i++ {
		values[i] = createFn(i)
	}

	return types.NewList(vrw, values...)
}

func buildListIncrementally(vrw types.ValueReadWriter, count uint64, createFn createValueFn) types.Collection {
	l := types.NewList(vrw).Edit()
	for i := uint64(0); i < count; i++ {
		l.Append(createFn(i))
	}

	return l.List()
}

func readList(c types.Collection) {
	c.(types.List).IterAll(func(v types.Value, idx uint64) {
	})
}

func buildSet(vrw types.ValueReadWriter, count uint64, createFn createValueFn) types.Collection {
	values := make([]types.Value, count)
	for i := uint64(0); i < count; i++ {
		values[i] = createFn(i)
	}

	return types.NewSet(vrw, values...)
}

func buildSetIncrementally(vrw types.ValueReadWriter, count uint64, createFn createValueFn) types.Collection {
	s := types.NewSet(vrw).Edit()
	for i := uint64(0); i < count; i++ {
		s.Insert(createFn(i))
	}

	return s.Set()
}

func readSet(c types.Collection) {
	c.(types.Set).IterAll(func(v types.Value) {
	})
}

func buildMap(vrw types.ValueReadWriter, count uint64, createFn createValueFn) types.Collection {
	values := make([]types.Value, count*2)
	for i := uint64(0); i < count*2; i++ {
		values[i] = createFn(i)
	}

	return types.NewMap(vrw, values...)
}

func buildMapIncrementally(vrw types.ValueReadWriter, count uint64, createFn createValueFn) types.Collection {
	me := types.NewMap(vrw).Edit()

	for i := uint64(0); i < count*2; i += 2 {
		me.Set(createFn(i), createFn(i+1))
	}

	return me.Map()
}

func readMap(c types.Collection) {
	c.(types.Map).IterAll(func(k types.Value, v types.Value) {
	})
}
