package main

import (
	"flag"
	"log"
	"runtime"

	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
)

var (
	p                = flag.Uint("p", 512, "parallelism")
	sinkDsFlags      = dataset.NewFlagsWithPrefix("sink-")
	sourceStoreFlags = datas.NewFlagsWithPrefix("source-")
	sourceObject     = flag.String("source", "", "source object to sync - either a dataset name or a ref")
)

func main() {
	cpuCount := runtime.NumCPU()
	runtime.GOMAXPROCS(cpuCount)

	flag.Parse()

	sourceStore, ok := sourceStoreFlags.CreateDataStore()
	sink := sinkDsFlags.CreateDataset()
	if !ok || sink == nil || *p == 0 || *sourceObject == "" {
		flag.Usage()
		return
	}
	defer sourceStore.Close()
	defer sink.Close()

	err := d.Try(func() {
		if util.MaybeStartCPUProfile() {
			defer util.StopCPUProfile()
		}

		sourceRef := ref.Ref{}
		if r, ok := ref.MaybeParse(*sourceObject); ok {
			if sourceStore.Has(r) {
				sourceRef = r
			}
		} else {
			if c, ok := sourceStore.MaybeHead(*sourceObject); ok {
				sourceRef = c.Ref()
			}
		}
		d.Exp.False(sourceRef.IsEmpty(), "Unknown source object: %s", *sourceObject)

		var err error
		*sink, err = sink.Pull(sourceStore, sourceRef, int(*p))

		util.MaybeWriteMemProfile()
		d.Exp.NoError(err)
	})

	if err != nil {
		log.Fatal(err)
	}
}
