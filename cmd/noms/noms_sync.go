// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"log"
	"time"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/profile"
	"github.com/attic-labs/noms/go/util/status"
	"github.com/attic-labs/noms/go/util/verbose"
	humanize "github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"
)

var (
	p int
)

var nomsSync = &util.Command{
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
	verbose.RegisterVerboseFlags(syncFlagSet)
	profile.RegisterProfileFlags(syncFlagSet)
	return syncFlagSet
}

func runSync(args []string) int {
	cfg := config.NewResolver()
	sourceStore, sourceObj, err := cfg.GetPath(args[0])
	d.CheckError(err)
	defer sourceStore.Close()

	if sourceObj == nil {
		d.CheckErrorNoUsage(fmt.Errorf("Object not found: %s", args[0]))
	}

	sinkDB, sinkDataset, err := cfg.GetDataset(args[1])
	d.CheckError(err)
	defer sinkDB.Close()

	start := time.Now()
	progressCh := make(chan datas.PullProgress)
	lastProgressCh := make(chan datas.PullProgress)

	go func() {
		var last datas.PullProgress

		for info := range progressCh {
			last = info
			if info.KnownCount == 1 {
				// It's better to print "up to date" than "0% (0/1); 100% (1/1)".
				continue
			}

			if status.WillPrint() {
				pct := 100.0 * float64(info.DoneCount) / float64(info.KnownCount)
				status.Printf("Syncing - %.2f%% (%s/s)", pct, bytesPerSec(info.ApproxWrittenBytes, start))
			}
		}
		lastProgressCh <- last
	}()

	sourceRef := types.NewRef(sourceObj)
	sinkRef, sinkExists := sinkDataset.MaybeHeadRef()
	nonFF := false
	err = d.Try(func() {
		defer profile.MaybeStartProfile().Stop()
		datas.Pull(sourceStore, sinkDB, sourceRef, progressCh)

		var err error
		sinkDataset, err = sinkDB.FastForward(sinkDataset, sourceRef)
		if err == datas.ErrMergeNeeded {
			sinkDataset, err = sinkDB.SetHead(sinkDataset, sourceRef)
			nonFF = true
		}
		d.PanicIfError(err)
	})

	if err != nil {
		log.Fatal(err)
	}

	close(progressCh)
	if last := <-lastProgressCh; last.DoneCount > 0 {
		status.Printf("Done - Synced %s in %s (%s/s)",
			humanize.Bytes(last.ApproxWrittenBytes), since(start), bytesPerSec(last.ApproxWrittenBytes, start))
		status.Done()
	} else if !sinkExists {
		fmt.Printf("All chunks already exist at destination! Created new dataset %s.\n", args[1])
	} else if nonFF && !sourceRef.Equals(sinkRef) {
		fmt.Printf("Abandoning %s; new head is %s\n", sinkRef.TargetHash(), sourceRef.TargetHash())
	} else {
		fmt.Printf("Dataset %s is already up to date.\n", args[1])
	}

	return 0
}

func bytesPerSec(bytes uint64, start time.Time) string {
	bps := float64(bytes) / float64(time.Since(start).Seconds())
	return humanize.Bytes(uint64(bps))
}

func since(start time.Time) string {
	round := time.Second / 100
	now := time.Now().Round(round)
	return now.Sub(start.Round(round)).String()
}
