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
	started := false
	if err := d.Try(func() { started = util.MaybeStartCPUProfile() }); started {
		defer util.StopCPUProfile()
	} else if err != nil {
		log.Fatalf("Can't create cpu profile file:\n%v\n", err)
	}

	newHead := source.Heads().Ref()
	var refs []ref.Ref
	if err := d.Try(func() { refs = sync.DiffHeadsByRef(sink.Heads().Ref(), newHead, source) }); err != nil {
		log.Fatalln(err)
	}
	if err := d.Try(func() { sync.CopyChunks(refs, source, sink) }); err != nil {
		log.Fatalln(err)
	}
	if err := d.Try(func() { sync.SetNewHeads(newHead, *sink) }); err != nil {
		log.Fatalln(err)
	}

	if err := d.Try(util.MaybeWriteMemProfile); err != nil {
		log.Fatalf("Can't create memory profile file:\n%v\n", err)
	}
}
