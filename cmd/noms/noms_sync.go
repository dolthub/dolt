// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/profile"
	"github.com/attic-labs/noms/go/util/status"
	humanize "github.com/dustin/go-humanize"
)

var (
	p int
)

var nomsSync = &nomsCommand{
	Run:       runSync,
	UsageLine: "sync [options] <source-object> <dest-dataset>",
	Short:     "Moves datasets between or within databases",
	Long:      "See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object and dataset arguments.",
	Flags:     setupSyncFlags,
	Nargs:     2,
}

func setupSyncFlags() *flag.FlagSet {
	syncFlagSet := flag.NewFlagSet("sync", flag.ExitOnError)
	syncFlagSet.IntVar(&p, "p", 512, "parallelism")
	spec.RegisterDatabaseFlags(syncFlagSet)
	profile.RegisterProfileFlags(syncFlagSet)
	return syncFlagSet
}

func runSync(args []string) int {

	sourceStore, sourceObj, err := spec.GetPath(args[0])
	d.CheckError(err)
	defer sourceStore.Close()

	sinkDataset, err := spec.GetDataset(args[1])
	d.CheckError(err)
	defer sinkDataset.Database().Close()

	start := time.Now()
	progressCh := make(chan datas.PullProgress)
	lastProgressCh := make(chan datas.PullProgress)

	go func() {
		var last datas.PullProgress

		for info := range progressCh {
			if info.KnownCount == 1 {
				// It's better to print "up to date" than "0% (0/1); 100% (1/1)".
				continue
			}

			last = info
			if status.WillPrint() {
				pct := 100.0 * float64(info.DoneCount) / float64(info.KnownCount)
				bytesPerSec := float64(info.DoneBytes) / float64(time.Since(start).Seconds())
				status.Printf("Syncing - %.2f%% (%d/%d chunks) - %s/s", pct, info.DoneCount, info.KnownCount, humanize.Bytes(uint64(bytesPerSec)))
			}
		}

		lastProgressCh <- last
	}()

	err = d.Try(func() {
		defer profile.MaybeStartProfile().Stop()
		var err error
		sinkDataset, err = sinkDataset.Pull(sourceStore, types.NewRef(sourceObj), p, progressCh)
		d.PanicIfError(err)
	})

	if err != nil {
		log.Fatal(err)
	}

	close(progressCh)
	if last := <-lastProgressCh; last.DoneCount > 0 {
		status.Printf("Done - Synced %s (%d chunks) in %s", humanize.Bytes(last.DoneBytes), last.DoneCount, time.Since(start).String())
		status.Done()
	} else {
		fmt.Println(flag.Arg(1), "is up to date.")
	}

	return 0
}
