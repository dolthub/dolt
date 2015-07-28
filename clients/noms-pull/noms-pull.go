package main

import (
	"flag"
	"log"
	"os"
	"runtime/pprof"

	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/sync"
)

var (
	cpuprofile    = flag.String("cpuprofile", "", "write cpu profile to file")
	localDsFlags  = dataset.NewFlagsWithPrefix("local-")
	remoteDsFlags = dataset.NewFlagsWithPrefix("remote-")
)

func main() {
	flag.Parse()

	source := remoteDsFlags.CreateDataset()
	sink := localDsFlags.CreateDataset()
	if source == nil || sink == nil {
		flag.Usage()
		return
	}
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		dbg.Chk.NoError(err, "Can't create cpu profile file.")
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	newHead := source.Heads().Ref()
	refs, err := sync.DiffHeadsByRef(sink.Heads().Ref(), newHead, source)
	if err != nil {
		log.Fatalln(err)
	}
	err = sync.CopyChunks(refs, source, sink)
	if err != nil {
		log.Fatalln(err)
	}
	_, err = sync.SetNewHeads(newHead, *sink)
	if err != nil {
		log.Fatalln(err)
	}
}
