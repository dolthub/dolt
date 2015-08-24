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

		newHead := source.Head().Ref()
		refs := sync.DiffHeadsByRef(sink.Head().Ref(), newHead, source.Store())
		sync.CopyChunks(refs, source.Store(), sink.Store())
		for ok := false; !ok; *sink, ok = sync.SetNewHead(newHead, *sink) {
			continue
		}

		util.MaybeWriteMemProfile()
	})

	if err != nil {
		log.Fatal(err)
	}
}
