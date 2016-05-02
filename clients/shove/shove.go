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
	"github.com/attic-labs/noms/types"
)

var (
	p             = flag.Uint("p", 512, "parallelism")
	sinkDsFlags   = dataset.NewFlagsWithPrefix("sink-")
	sourceDbFlags = datas.NewFlagsWithPrefix("source-")
	sourceObject  = flag.String("source", "", "source object to sync - either a dataset name or a ref")
)

func main() {
	cpuCount := runtime.NumCPU()
	runtime.GOMAXPROCS(cpuCount)

	flag.Parse()

	sourceDb, ok := sourceDbFlags.CreateDatabase()
	sink := sinkDsFlags.CreateDataset()
	if !ok || sink == nil || *p == 0 || *sourceObject == "" {
		flag.Usage()
		return
	}
	defer sourceDb.Close()
	defer sink.DB().Close()

	err := d.Try(func() {
		if util.MaybeStartCPUProfile() {
			defer util.StopCPUProfile()
		}

		var commit types.Struct
		if r, ok := ref.MaybeParse(*sourceObject); ok {
			// sourceObject was sha1
			commit, ok = sourceDb.ReadValue(r).(types.Struct)
			d.Exp.True(ok, "Unable to read Commit object with ref: %s", r)
		} else {
			// sourceObject must be a dataset Id
			commit, ok = sourceDb.MaybeHead(*sourceObject)
			d.Exp.True(ok, "Unable to read dataset with name: %s", *sourceObject)
		}

		var err error
		*sink, err = sink.Pull(sourceDb, types.NewTypedRefFromValue(commit), int(*p))

		util.MaybeWriteMemProfile()
		d.Exp.NoError(err)
	})

	if err != nil {
		log.Fatal(err)
	}
}
