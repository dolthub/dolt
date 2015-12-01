package main

import (
	"flag"
	"log"
	"runtime"

	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
)

var (
	sinkDsFlags   = dataset.NewFlagsWithPrefix("sink-")
	sourceDsFlags = dataset.NewFlagsWithPrefix("source-")
	p             = flag.Uint("p", 512, "parallelism")
	sinkRefFlag   = flag.String("sinkref", "", "ref to use in place of sink dataset head (useful for testing)")
)

func main() {
	cpuCount := runtime.NumCPU()
	runtime.GOMAXPROCS(cpuCount)

	flag.Parse()

	source := sourceDsFlags.CreateDataset()
	sink := sinkDsFlags.CreateDataset()
	if source == nil || sink == nil || *p == 0 {
		flag.Usage()
		return
	}
	defer source.Close()
	defer sink.Close()

	err := d.Try(func() {
		if util.MaybeStartCPUProfile() {
			defer util.StopCPUProfile()
		}

		sinkRef := ref.Ref{}
		if *sinkRefFlag != "" {
			sinkRef = ref.Parse(*sinkRefFlag)
		}

		*sink = sink.Pull(*source, int(*p), sinkRef)

		util.MaybeWriteMemProfile()
	})

	if err != nil {
		log.Fatal(err)
	}
}
