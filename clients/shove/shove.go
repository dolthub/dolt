package main

import (
	"flag"
	"log"

	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
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
	defer source.Close()
	defer sink.Close()

	err := d.Try(func() {
		if util.MaybeStartCPUProfile() {
			defer util.StopCPUProfile()
		}

		newHeadRef := source.Head().Ref()
		currentHeadRef := ref.Ref{}
		if currentHead, ok := sink.MaybeHead(); ok {
			currentHeadRef = currentHead.Ref()
		}
		refs := sync.DiffHeadsByRef(currentHeadRef, newHeadRef, source.Store())
		sync.CopyChunks(refs, source.Store(), sink.Store())
		for ok := false; !ok; *sink, ok = sync.SetNewHead(newHeadRef, *sink) {
			continue
		}

		util.MaybeWriteMemProfile()
	})

	if err != nil {
		log.Fatal(err)
	}
}
