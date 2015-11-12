package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/walk"
)

var (
	flags    = datas.NewFlags()
	inputID  = flag.String("in", "", "dataset to find photos in")
	outputID = flag.String("out", "", "dataset to store index in")
)

type targetRef interface {
	TargetRef() ref.Ref
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Usage = func() {
		fmt.Printf("Usage: %s -ldb=/path/to/db -in=flickr -out=tagdex\n\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	store, ok := flags.CreateDataStore()
	if !ok || *inputID == "" || *outputID == "" {
		flag.Usage()
		return
	}
	defer store.Close()

	inputDS := dataset.NewDataset(store, *inputID)
	commit, ok := inputDS.MaybeHead()
	if !ok {
		log.Fatalf("No dataset named %s", *inputID)
	}
	outputDS := dataset.NewDataset(store, *outputID)

	out := NewMapOfStringToSetOfRefOfRemotePhoto()

	t0 := time.Now()
	numValues := 0
	numPhotos := 0

	walk.AllP(commit.Value(), store, func(v types.Value) {
		numValues++
		if p, ok := v.(RemotePhoto); ok {
			tags := p.Tags()
			if !tags.Empty() {
				numPhotos++
				fmt.Println("Indexing", p.Ref())

				r := NewRefOfRemotePhoto(p.Ref())
				tags.IterAll(func(item string) {
					s, _ := out.MaybeGet(item)
					out = out.Set(item, s.Insert(r))
				})
			}
		}

		// AllP gives inconsistent results. BUG 604
	}, 1)

	_, ok = outputDS.Commit(out)
	d.Exp.True(ok, "Could not commit due to conflicting edit")

	fmt.Printf("Indexed %v photos from %v values in %v\n", numPhotos, numValues, time.Now().Sub(t0))
}
