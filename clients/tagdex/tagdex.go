package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
)

var (
	csFlags  = chunks.NewFlags()
	inputID  = flag.String("input-ds", "", "dataset to find photos within")
	outputID = flag.String("output-ds", "", "dataset to store index in")
)

func main() {
	flag.Parse()

	cs := csFlags.CreateStore()
	if cs == nil || *inputID == "" || *outputID == "" {
		flag.Usage()
		return
	}
	defer cs.Close()

	store := datas.NewDataStore(cs)
	inputDS := dataset.NewDataset(store, *inputID)
	if _, ok := inputDS.MaybeHead(); !ok {
		log.Fatalf("No dataset named %s", *inputID)
	}
	outputDS := dataset.NewDataset(store, *outputID)

	out := NewMapOfStringToSet()

	t0 := time.Now()
	numRefs := 0
	numPhotos := 0

	types.Some(inputDS.Head().Value().Ref(), cs, func(f types.Future) (skip bool) {
		numRefs++
		v := f.Deref(cs)
		if v, ok := v.(types.Map); ok {
			name := v.Get(types.NewString("$name"))
			if name == nil {
				return
			}

			if !name.Equals(types.NewString("Photo")) && !name.Equals(types.NewString("RemotePhoto")) {
				return
			}

			skip = true
			numPhotos++
			fmt.Println("Indexing", v.Ref())

			tags := SetOfStringFromVal(v.Get(types.NewString("tags")))
			tags.Iter(func(item types.String) (stop bool) {
				var s types.Set
				if out.Has(item) {
					s = out.Get(item)
				} else {
					s = types.NewSet()
				}
				out = out.Set(item, s.Insert(v))
				return
			})
		}
		return
	})

	_, ok := outputDS.Commit(out.NomsValue())
	d.Exp.True(ok, "Could not commit due to conflicting edit")

	fmt.Printf("Indexed %v photos from %v refs in %v\n", numPhotos, numRefs, time.Now().Sub(t0))
}
