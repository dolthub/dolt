package main

import (
	"flag"
	"log"

	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/sync"
)

var (
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

	err := d.Try(func() {
		if util.MaybeStartCPUProfile() {
			defer util.StopCPUProfile()
		}

		newHead := source.Heads().Ref()
		refs := sync.DiffHeadsByRef(sink.Heads().Ref(), newHead, source)
		sync.CopyChunks(refs, source, sink)
		sync.SetNewHeads(newHead, *sink)

		util.MaybeWriteMemProfile()
	})

	if err != nil {
		log.Fatal(err)
	}
}
