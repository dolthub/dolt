// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/ipfs"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/samples/go/ipfs-chat/dbg"
	"github.com/attic-labs/noms/samples/go/ipfs-chat/lib"
)

var (
	topic    = flag.String("topic", "ipfs-chat", "topic to subscribe to for notifications of changes")
	interval = flag.Duration("interval", 5*time.Second, "rate to publish current head to network")
)

func main() {
	flag.Parse()

	flag.Usage = func() {
		fmt.Println("ipfs-chatd [flags] <ipfs-dataset> <ipfs-local-dataset>")
		flag.PrintDefaults()
		return
	}

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "--topic cannot be empty")
		return
	}

	if *interval == time.Duration(0) {
		fmt.Fprintln(os.Stderr, "--interval cannot be empty")
		return
	}

	if flag.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "Insufficient arguments")
		return
	}

	dbg.SetLogger(log.New(os.Stdout, "", 0))

	sourceSp, err := spec.ForDataset(flag.Arg(0))
	d.CheckErrorNoUsage(err)
	source := sourceSp.GetDataset()
	source, err = lib.InitDatabase(source)
	d.PanicIfError(err)

	destSp, err := spec.ForDataset(flag.Arg(1))
	d.CheckErrorNoUsage(err)
	dest := destSp.GetDataset()
	dest, err = lib.InitDatabase(dest)
	d.PanicIfError(err)

	fmt.Printf("Replicating %s to %s...\n", sourceSp.String(), destSp.String())

	sub, err := ipfs.CurrentNode.Floodsub.Subscribe(*topic)
	d.PanicIfError(err)
	go lib.Replicate(sub, source, dest, func(ds datas.Dataset) {
		dest = ds
	})

	for {
		s := dest.HeadRef().TargetHash().String()
		fmt.Println("publishing: " + s)
		lib.Publish(sub, *topic, s)
		time.Sleep(*interval)
	}
}
