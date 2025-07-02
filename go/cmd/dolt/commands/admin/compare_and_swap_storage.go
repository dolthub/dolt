// Copyright 2024 Dolthub, Inc.
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

package admin

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

type CompareAndSwapStorageCmd struct {
}

func (cmd CompareAndSwapStorageCmd) Name() string {
	return "compare-and-swap-storage"
}

var compareAndSwapDocs = cli.CommandDocumentationContent{
	ShortDesc: "Perform compare-and-swap operations on storage.",
	LongDesc:  `Run this command on a dolt database to perform compare-and-swap operations on storage. This command will compare storage states and swap as needed.`,

	Synopsis: []string{
		`--add <table id> --drop <table id>`,
	},
}

// Description returns a description of the command
func (cmd CompareAndSwapStorageCmd) Description() string {
	return "Hidden command to perform compare-and-swap operations on storage."
}
func (cmd CompareAndSwapStorageCmd) RequiresRepo() bool {
	return true
}
func (cmd CompareAndSwapStorageCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(compareAndSwapDocs, ap)
}

func (cmd CompareAndSwapStorageCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsString("add", "", "Table ID to add to the storage archive.", "")
	ap.SupportsString("drop", "", "Table ID to drop from the storage archive.", "")
	return ap
}
func (cmd CompareAndSwapStorageCmd) Hidden() bool {
	return true
}

func (cmd CompareAndSwapStorageCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, compareAndSwapDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	db := doltdb.HackDatasDatabaseFromDoltDB(dEnv.DoltDB(ctx))
	cs := datas.ChunkStoreFromDatabase(db)
	if _, ok := cs.(*nbs.GenerationalNBS); !ok {
		cli.PrintErrln("compare-and-swap-storage command requires a GenerationalNBS")
		return 1
	}

	addHashStr, ok := apr.GetValue("add")
	if !ok {
		cli.PrintErrln("compare-and-swap-storage command requires --add argument")
		return 1
	}
	addHash := hash.Parse(addHashStr)

	dropHashStr, ok := apr.GetValue("drop")
	if !ok {
		cli.PrintErrln("compare-and-swap-storage command requires --drop argument")
		return 1
	}
	dropHash := hash.Parse(dropHashStr)

	wg := sync.WaitGroup{}
	progress := make(chan interface{}, 32)
	handleProgress(ctx, &wg, progress)

	defer func() {
		close(progress)
		wg.Wait()
	}()

	err := nbs.CompareAndSwapStorage(ctx, cs, addHash, dropHash, progress)
	if err != nil {
		cli.PrintErrf("Error during compare-and-swap: %v\n", err)
		return 1
	}
	return 0
}

// NM4 - update to be more cas specfific.
func handleProgress(ctx context.Context, wg *sync.WaitGroup, progress chan interface{}) {
	go func() {
		wg.Add(1)
		defer wg.Done()

		rotation := 0
		p := cli.NewEphemeralPrinter()
		currentMessage := "Starting Archive Build"
		var lastProgressMsg *nbs.ArchiveBuildProgressMsg
		lastUpdateTime := time.Now()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-progress:
				if !ok {
					return
				}
				switch v := msg.(type) {
				case string:
					cli.Printf("%s\n", v)
				case nbs.ArchiveBuildProgressMsg:
					if v.Total == v.Completed {
						p.Printf("%s: Done\n", v.Stage)
						lastProgressMsg = nil
						currentMessage = ""
						p.Display()
						cli.Printf("\n")
					} else {
						lastProgressMsg = &v
					}
				default:
					cli.Printf("Unexpected Message: %v\n", v)
				}
			// If no events come in, we still want to update the progress bar every second.
			case <-time.After(1 * time.Second):
			}

			if now := time.Now(); now.Sub(lastUpdateTime) > 1*time.Second {
				rotation++
				switch rotation % 4 {
				case 0:
					p.Printf("- ")
				case 1:
					p.Printf("\\ ")
				case 2:
					p.Printf("| ")
				case 3:
					p.Printf("/ ")
				}

				if lastProgressMsg != nil {
					percentDone := 0.0
					totalCount := lastProgressMsg.Total
					if lastProgressMsg.Total > 0 {
						percentDone = float64(lastProgressMsg.Completed) / float64(lastProgressMsg.Total)
						percentDone *= 100.0
					}

					currentMessage = fmt.Sprintf("%s: %d/%d (%.2f%%)", lastProgressMsg.Stage, lastProgressMsg.Completed, totalCount, percentDone)
				}

				p.Printf("%s", currentMessage) // Don't update message, but allow ticker to turn.
				lastUpdateTime = now

				p.Display()
			}
		}
	}()
}
