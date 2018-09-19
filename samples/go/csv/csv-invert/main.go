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
		types.FloatKind:  types.Float(0),
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
	streams := map[string]*stream{}
	lists := map[string]<-chan types.List{}
	lowers := map[string]string{}

	sDesc := types.TypeOf(l).Desc.(types.CompoundDesc).ElemTypes[0].Desc.(types.StructDesc)
	sDesc.IterFields(func(name string, t *types.Type, optional bool) {
		lowerName := strings.ToLower(name)
		if _, present := streams[lowerName]; !present {
			s := &stream{make(chan types.Value, 1024), zeroVal(t)}
			streams[lowerName] = s
			lists[lowerName] = types.NewStreamingList(outDB, s.ch)
		}
		lowers[name] = lowerName
	})

	filledCols := make(map[string]struct{}, len(streams))
	l.IterAll(func(v types.Value, index uint64) {
		// First, iterate the fields that are present in |v| and append values to the correct lists
		v.(types.Struct).IterFields(func(name string, value types.Value) {
			ln := lowers[name]
			filledCols[ln] = struct{}{}
			streams[ln].ch <- value
		})
		// Second, iterate all the streams, skipping the ones we already sent a value for, and send an empty String for the remaining ones.
		for lowerName, stream := range streams {
			if _, present := filledCols[lowerName]; present {
				delete(filledCols, lowerName)
				continue
			}
			stream.ch <- stream.zeroVal
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
