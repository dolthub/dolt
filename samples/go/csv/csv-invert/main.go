// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/profile"
	flag "github.com/juju/gnuflag"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <dataset-to-invert> <output-dataset>\n", os.Args[0])
		flag.PrintDefaults()
	}

	profile.RegisterProfileFlags(flag.CommandLine)
	flag.Parse(true)

	if flag.NArg() != 2 {
		flag.Usage()
		return
	}

	cfg := config.NewResolver()
	inDB, inDS, err := cfg.GetDataset(flag.Arg(0))
	d.CheckError(err)
	defer inDB.Close()

	head, present := inDS.MaybeHead()
	if !present {
		d.CheckErrorNoUsage(fmt.Errorf("The dataset %s has no head", flag.Arg(0)))
	}
	v := head.Get(datas.ValueField)
	l, isList := v.(types.List)
	if !isList {
		d.CheckErrorNoUsage(fmt.Errorf("The head value of %s is not a list, but rather %s", flag.Arg(0), types.TypeOf(v).Describe()))
	}

	outDB, outDS, err := cfg.GetDataset(flag.Arg(1))
	defer outDB.Close()

	// I don't want to allocate a new types.Value every time someone calls zeroVal(), so instead have a map of canned Values to reference.
	zeroVals := map[types.NomsKind]types.Value{
		types.BoolKind:   types.Bool(false),
		types.NumberKind: types.Number(0),
		types.StringKind: types.String(""),
	}

	zeroVal := func(t *types.Type) types.Value {
		v, present := zeroVals[t.TargetKind()]
		if !present {
			d.CheckErrorNoUsage(fmt.Errorf("csv-invert doesn't support values of type %s", t.Describe()))
		}
		return v
	}

	defer profile.MaybeStartProfile().Stop()
	type stream struct {
		ch      chan types.Value
		zeroVal types.Value
	}
	streams := map[string]stream{}
	lists := map[string]<-chan types.List{}
	lowers := map[string]string{}

	sDesc := types.TypeOf(l).Desc.(types.CompoundDesc).ElemTypes[0].Desc.(types.StructDesc)
	sDesc.IterFields(func(name string, t *types.Type, optional bool) {
		lowerName := strings.ToLower(name)
		if _, present := streams[lowerName]; !present {
			s := stream{make(chan types.Value, 1024), zeroVal(t)}
			streams[lowerName] = s
			lists[lowerName] = types.NewStreamingList(outDB, s.ch)
		}
		lowers[name] = lowerName
	})

	columnVals := make(map[string]types.Value, len(streams))
	l.IterAll(func(v types.Value, index uint64) {
		for lowerName, stram := range streams {
			columnVals[lowerName] = stram.zeroVal
		}
		v.(types.Struct).IterFields(func(name string, value types.Value) {
			columnVals[lowers[name]] = value
		})
		for lowerName, stream := range streams {
			stream.ch <- columnVals[lowerName]
		}
	})

	invertedStructData := types.StructData{}
	for lowerName, stream := range streams {
		close(stream.ch)
		invertedStructData[lowerName] = <-lists[lowerName]
	}
	str := types.NewStruct("Columnar", invertedStructData)

	parents := types.NewSet(outDB)
	if headRef, present := outDS.MaybeHeadRef(); present {
		parents = types.NewSet(outDB, headRef)
	}

	_, err = outDB.Commit(outDS, str, datas.CommitOptions{Parents: parents, Meta: head.Get(datas.MetaField).(types.Struct)})
	d.PanicIfError(err)
}
