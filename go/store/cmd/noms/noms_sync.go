// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"

	"github.com/dolthub/dolt/go/store/cmd/noms/util"
	"github.com/dolthub/dolt/go/store/config"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/datas/pull"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/profile"
	"github.com/dolthub/dolt/go/store/util/status"
	"github.com/dolthub/dolt/go/store/util/verbose"
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

func runSync(ctx context.Context, args []string) int {
	cfg := config.NewResolver()
	sourceStore, sourceVRW, sourceObj, err := cfg.GetPath(ctx, args[0])
	util.CheckError(err)
	defer sourceStore.Close()

	if sourceObj == nil {
		util.CheckErrorNoUsage(fmt.Errorf("Object not found: %s", args[0]))
	}

	sinkDB, _, sinkDataset, err := cfg.GetDataset(ctx, args[1])
	util.CheckError(err)
	defer sinkDB.Close()

	start := time.Now()
	progressCh := make(chan pull.PullProgress)
	lastProgressCh := make(chan pull.PullProgress)

	go func() {
		var last pull.PullProgress

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

	sourceRef, err := types.NewRef(sourceObj, sourceVRW.Format())
	util.CheckError(err)
	sinkAddr, sinkExists := sinkDataset.MaybeHeadAddr()
	nonFF := false
	srcCS := datas.ChunkStoreFromDatabase(sourceStore)
	sinkCS := datas.ChunkStoreFromDatabase(sinkDB)
	waf, err := types.WalkAddrsForChunkStore(srcCS)
	util.CheckError(err)
	f := func() error {
		defer profile.MaybeStartProfile().Stop()
		addr := sourceRef.TargetHash()
		err := pull.Pull(ctx, srcCS, sinkCS, waf, addr, progressCh)

		if err != nil {
			return err
		}

		var tempDS datas.Dataset
		tempDS, err = sinkDB.FastForward(ctx, sinkDataset, sourceRef.TargetHash())
		if err == datas.ErrMergeNeeded {
			sinkDataset, err = sinkDB.SetHead(ctx, sinkDataset, addr)
			nonFF = true
		} else if err == nil {
			sinkDataset = tempDS
		}

		return err
	}

	err = f()

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
	} else if nonFF && sourceRef.TargetHash() != sinkAddr {
		fmt.Printf("Abandoning %s; new head is %s\n", sinkAddr, sourceRef.TargetHash())
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
