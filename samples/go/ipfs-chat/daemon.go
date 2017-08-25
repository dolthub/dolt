// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/ipfs"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/samples/go/ipfs-chat/dbg"
)

func runDaemon(topic string, interval time.Duration, networkDS, localDS string) {
	dbg.SetLogger(log.New(os.Stdout, "", 0))

	sourceSp, err := spec.ForDataset(networkDS)
	d.CheckErrorNoUsage(err)
	source := sourceSp.GetDataset()
	source, err = InitDatabase(source)
	d.PanicIfError(err)

	destSp, err := spec.ForDataset(localDS)
	d.CheckErrorNoUsage(err)
	dest := destSp.GetDataset()
	dest, err = InitDatabase(dest)
	d.PanicIfError(err)

	fmt.Printf("Replicating %s to %s...\n", sourceSp.String(), destSp.String())

	sub, err := ipfs.CurrentNode.Floodsub.Subscribe(topic)
	d.PanicIfError(err)
	go Replicate(sub, source, dest, func(ds datas.Dataset) {
		dest = ds
	})

	for {
		s := dest.HeadRef().TargetHash().String()
		fmt.Println("publishing: " + s)
		Publish(sub, topic, s)
		time.Sleep(interval)
	}
}
