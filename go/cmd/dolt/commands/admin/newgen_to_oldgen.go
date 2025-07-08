// Copyright 2025 Dolthub, Inc.
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
	"os"
	"sync"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
)

type NewGenToOldGenCmd struct {
}

func (cmd NewGenToOldGenCmd) Name() string {
	return "newgen-to-oldgen"
}

var newGenToOldGenDocs = cli.CommandDocumentationContent{
	ShortDesc: "Promote everything in newgen to oldgen. Skips GC.",
	LongDesc:  `Admin command to force the promotion of all table files in the new generation to the old generation.`,
	Synopsis:  []string{},
}

// Description returns a description of the command
func (cmd NewGenToOldGenCmd) Description() string {
	return "Admin command to move all storage from newgen to oldgen."
}
func (cmd NewGenToOldGenCmd) RequiresRepo() bool {
	return true
}
func (cmd NewGenToOldGenCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(newGenToOldGenDocs, ap)
}

func (cmd NewGenToOldGenCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	return ap
}
func (cmd NewGenToOldGenCmd) Hidden() bool {
	return true
}

func (cmd NewGenToOldGenCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	db := doltdb.HackDatasDatabaseFromDoltDB(dEnv.DoltDB(ctx))
	cs := datas.ChunkStoreFromDatabase(db)
	if _, ok := cs.(*nbs.GenerationalNBS); !ok {
		cli.PrintErrln("compare-and-swap-storage command requires a GenerationalNBS")
		return 1
	}

	wg := sync.WaitGroup{}
	progress := make(chan string, 32)
	handleNewGenToOldGenProgress(ctx, &wg, progress)

	defer func() {
		close(progress)
		wg.Wait()
		os.Stdout.Sync()
		os.Stderr.Sync()
	}()

	err := nbs.MoveNewGenToOldGen(ctx, cs, progress)
	if err != nil {
		cli.PrintErrf("Error during newgen-to-oldgen: %v\n", err)
		return 1
	}
	return 0
}

// NM4 - update to be more cas specfific.
func handleNewGenToOldGenProgress(ctx context.Context, wg *sync.WaitGroup, progress chan string) {
	go func() {
		wg.Add(1)
		defer wg.Done()

		rotation := 0
		p := cli.NewEphemeralPrinter()
		currentMessage := "Converting NewGen to OldGen"
		lastUpdateTime := time.Now()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-progress:
				if !ok {
					return
				}
				currentMessage = msg
				cli.Printf("%s\n", msg)
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

				p.Printf("%s", currentMessage) // Don't update message, but allow ticker to turn.
				lastUpdateTime = now

				p.Display()
			}
		}
	}()
}
