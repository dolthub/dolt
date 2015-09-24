package main

import (
	"flag"
	"log"
	"runtime"

	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
)

var (
	sinkDsFlags   = dataset.NewFlagsWithPrefix("sink-")
	sourceDsFlags = dataset.NewFlagsWithPrefix("source-")
	p             = flag.Uint("p", 512, "parallelism")
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

		*sink = sink.Pull(*source, int(*p))

		util.MaybeWriteMemProfile()
	})

	if err != nil {
		log.Fatal(err)
	}
}
