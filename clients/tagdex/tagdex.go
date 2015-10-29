package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

var (
	flags        = datas.NewFlags()
	inputRefFlag = flag.String("input-ref", "", "ref to root of a set of photos")
	outputID     = flag.String("output-ds", "", "dataset to store index in")
)

type targetRef interface {
	TargetRef() ref.Ref
}

func main() {
	flag.Parse()

	store, ok := flags.CreateDataStore()
	if !ok || *inputRefFlag == "" || *outputID == "" {
		flag.Usage()
		return
	}
	defer store.Close()

	var photoSet SetOfRefOfRemotePhoto
	if d.Try(func() {
		r := ref.Parse(*inputRefFlag)
		photoSet = types.ReadValue(r, store).(SetOfRefOfRemotePhoto)
	}) != nil {
		log.Fatal("Invalid Ref: %s\n", *inputRefFlag)
	}

	outputDS := dataset.NewDataset(store, *outputID)

	out := NewMapOfStringToSetOfRefOfRemotePhoto()

	t0 := time.Now()
	numRefs := 0
	numPhotos := 0

	photoSet.IterAll(func(r RefOfRemotePhoto) {
		numRefs++

		p := r.TargetValue(store)
		tags := p.Tags()

		if !tags.Empty() {
			numPhotos++
			fmt.Println("Indexing", p.Ref())

			tags.IterAll(func(item string) {
				s, _ := out.MaybeGet(item)
				out = out.Set(item, s.Insert(r))
			})
		}
	})

	_, ok = outputDS.Commit(out)
	d.Exp.True(ok, "Could not commit due to conflicting edit")

	fmt.Printf("Indexed %v photos from %v refs in %v\n", numPhotos, numRefs, time.Now().Sub(t0))
}
