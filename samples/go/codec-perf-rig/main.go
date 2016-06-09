// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"runtime/pprof"
	"time"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/profile"
	"github.com/attic-labs/noms/samples/go/flags"
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
	flags.RegisterDatabaseFlags()
	flag.Parse()

	profiling := profile.MaybeStartCPUProfile()

	collectionTypes := []string{"List", "Set", "Map"}
	buildOneFns := []buildCollectionFn{buildList, buildSet, buildMap}
	buildIncrFns := []buildCollectionFn{buildListIncrementally, buildSetIncrementally, buildMapIncrementally}
	readFns := []readCollectionFn{readList, readSet, readMap}

	elementTypes := []string{"numbers (8 B)", "strings (32 B)", "structs (64 B)"}
	elementSizes := []uint64{numberSize, stringSize, structSize}
	createValueFns := []createValuesFn{createNumbers, createStrings, createStructs}

	for i, colType := range collectionTypes {
		buildCount := *count
		appendCount := *count / 50
		fmt.Printf("Testing %s: \t\tbuild %d\t\t\tscan %d\t\t\tinsert %d\n", colType, buildCount, buildCount, appendCount)

		for j, elementType := range elementTypes {
			elementSize := elementSizes[j]
			buildSize := elementSize * buildCount
			incrSize := elementSize * appendCount

			valueFn := createValueFns[j]
			var vals types.ValueSlice

			// Build One-Time
			ms := chunks.NewMemoryStore()
			ds := dataset.NewDataset(datas.NewDatabase(ms), "test")
			if colType == "Map" {
				vals = valueFn(buildCount * 2)
				buildSize *= 2
				incrSize *= 2
			} else {
				vals = valueFn(buildCount)
			}
			t1 := time.Now()
			col := buildOneFns[i](vals)
			ds, err := ds.Commit(col)
			d.Chk.NoError(err)
			buildDuration := time.Since(t1)

			// Read
			if i == 1 && j == 1 {
				f, err := os.Create("cpu.prof")
				d.Exp.NoError(err)
				pprof.StartCPUProfile(f)
			}

			t1 = time.Now()
			col = ds.Head().Get(datas.ValueField).(types.Collection)
			readFns[i](col)
			readDuration := time.Since(t1)
			if i == 1 && j == 1 {
				pprof.StopCPUProfile()
			}

			// Build Incrementally
			ms = chunks.NewMemoryStore()
			ds = dataset.NewDataset(datas.NewDatabase(ms), "test")
			if colType == "Map" {
				vals = valueFn(appendCount * 2)
			} else {
				vals = valueFn(appendCount)
			}
			t1 = time.Now()
			col = buildIncrFns[i](vals)
			ds, err = ds.Commit(col)
			d.Chk.NoError(err)
			incrDuration := time.Since(t1)

			fmt.Printf("%s\t\t%s\t\t%s\t\t%s\n", elementType, rate(buildDuration, buildSize), rate(readDuration, buildSize), rate(incrDuration, incrSize))
		}
		fmt.Println()
	}

	fmt.Printf("Testing blob: \t\tbuild %d MB\t\t\tscan %d MB\n", *blobSize/1000000, *blobSize/1000000)

	ms := chunks.NewMemoryStore()
	ds := dataset.NewDataset(datas.NewDatabase(ms), "test")

	blobBytes := makeBlobBytes(*blobSize)
	t1 := time.Now()
	blob := types.NewBlob(bytes.NewReader(blobBytes))
	ds.Commit(blob)
	buildDuration := time.Since(t1)

	t1 = time.Now()
	outBytes, _ := ioutil.ReadAll(blob.Reader())
	readDuration := time.Since(t1)
	d.Chk.True(bytes.Compare(blobBytes, outBytes) == 0)
	fmt.Printf("\t\t\t%s\t\t%s\n\n", rate(buildDuration, *blobSize), rate(readDuration, *blobSize))

	if profiling {
		profile.StopCPUProfile()
	}
}

func rate(d time.Duration, size uint64) string {
	return fmt.Sprintf("%d ms (%.2f MB/s)", uint64(d)/1000000, float64(size)*1000/float64(d))
}

type createValuesFn func(count uint64) types.ValueSlice
type buildCollectionFn func(values types.ValueSlice) types.Collection
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
func createStrings(count uint64) types.ValueSlice {
	values := make(types.ValueSlice, count)
	for i := uint64(0); i < count; i++ {
		values[i] = types.NewString(fmt.Sprintf("%s%d", strPrefix, i))
	}

	return values
}

func createNumbers(count uint64) types.ValueSlice {
	values := make(types.ValueSlice, count)
	for i := uint64(0); i < count; i++ {
		values[i] = types.Number(i)
	}

	return values
}

var structType = types.MakeStructType("S1", map[string]*types.Type{
	"str":  types.StringType,
	"num":  types.NumberType,
	"bool": types.BoolType,
})

func createStructs(count uint64) types.ValueSlice {
	values := make(types.ValueSlice, count)
	for i := uint64(0); i < count; i++ {
		values[i] = types.NewStructWithType(structType, map[string]types.Value{
			"str":  types.NewString(fmt.Sprintf("i am a 55 bytes............................%12d", strPrefix, i)),
			"num":  types.Number(i),
			"bool": types.Bool(i%2 == 0),
		})
	}

	return values
}

func buildList(values types.ValueSlice) types.Collection {
	return types.NewList(values...)
}

func buildListIncrementally(values types.ValueSlice) types.Collection {
	l := types.NewList()
	for i, v := range values {
		l = l.Insert(uint64(i), v)
	}

	return l
}

func readList(c types.Collection) {
	var outValue types.Value
	c.(types.List).IterAll(func(v types.Value, idx uint64) {
		outValue = v
	})
}

func buildSet(values types.ValueSlice) types.Collection {
	return types.NewSet(values...)
}

func buildSetIncrementally(values types.ValueSlice) types.Collection {
	s := types.NewSet()
	for _, v := range values {
		s = s.Insert(v)
	}

	return s
}

func readSet(c types.Collection) {
	var outValue types.Value
	c.(types.Set).IterAll(func(v types.Value) {
		v = outValue
	})
}

func buildMap(values types.ValueSlice) types.Collection {
	return types.NewMap(values...)
}

func buildMapIncrementally(values types.ValueSlice) types.Collection {
	m := types.NewMap()
	for i := 0; i < len(values); i += 2 {
		m = m.Set(values[i], values[i+1])
	}

	return m
}

func readMap(c types.Collection) {
	var outKey, outValue types.Value
	c.(types.Map).IterAll(func(k types.Value, v types.Value) {
		outKey = k
		outValue = v
	})
}
