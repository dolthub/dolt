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

package commands

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly/tree"
)

type ArchiveCmd struct {
}

func (cmd ArchiveCmd) Name() string {
	return "archive"
}

var docs = cli.CommandDocumentationContent{
	ShortDesc: "Create archive files for greater compression, then verify all chunks.",
	LongDesc: `Run this command on a dolt database only after running 'dolt gc'. This command will convert all 'oldgen' 
table files into archives. Currently, for safety, table files are left in place.`,

	Synopsis: []string{
		`[--group-chunks]`,
	},
}

const groupChunksFlag = "group-chunks"
const revertFlag = "revert"
const purgeFlag = "purge"

// Description returns a description of the command
func (cmd ArchiveCmd) Description() string {
	return "Hidden command to kick the tires with the new archive format."
}
func (cmd ArchiveCmd) RequiresRepo() bool {
	return true
}
func (cmd ArchiveCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(docs, ap)
}

func (cmd ArchiveCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsFlag(groupChunksFlag, "", "Attempt to group chunks. This will produce smaller archives, but can take much longer to build.")
	ap.SupportsFlag(revertFlag, "", "Return to unpurged table files, or rebuilt table files from archives")
	ap.SupportsFlag(purgeFlag, "", "remove table files after archiving")
	return ap
}
func (cmd ArchiveCmd) Hidden() bool {
	return true
}

func (cmd ArchiveCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, docs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	db := doltdb.HackDatasDatabaseFromDoltDB(dEnv.DoltDB(ctx))
	cs := datas.ChunkStoreFromDatabase(db)
	if _, ok := cs.(*nbs.GenerationalNBS); !ok {
		cli.PrintErrln("archive command requires a GenerationalNBS")
		return 1
	}

	storageMetadata, err := env.GetMultiEnvStorageMetadata(ctx, dEnv.FS)
	if err != nil {
		cli.PrintErrln(err)
		return 1
	}
	if len(storageMetadata) != 1 {
		cli.PrintErrln("Runtime error: Multiple databases found where one expected")
		return 1
	}
	var ourDbMD nbs.StorageMetadata
	for _, md := range storageMetadata {
		ourDbMD = md
	}

	progress := make(chan interface{}, 32)
	handleProgress(ctx, progress)

	if apr.Contains(revertFlag) {
		err := nbs.UnArchive(ctx, cs, ourDbMD, progress)
		if err != nil {
			cli.PrintErrln(err)
			return 1
		}
	} else {
		datasets, err := db.Datasets(ctx)
		if err != nil {
			cli.PrintErrln(err)
			return 1
		}

		hs := hash.NewHashSet()
		err = datasets.IterAll(ctx, func(id string, hash hash.Hash) error {
			hs.Insert(hash)
			return nil
		})

		groupings := nbs.NewChunkRelations()
		if apr.Contains(groupChunksFlag) {
			err = historicalFuzzyMatching(ctx, hs, &groupings, dEnv.DoltDB(ctx))
			if err != nil {
				cli.PrintErrln(err)
				return 1
			}
		}

		purge := apr.Contains(purgeFlag)

		err = nbs.BuildArchive(ctx, cs, &groupings, purge, progress)
		if err != nil {
			cli.PrintErrln(err)
			return 1
		}

	}
	return 0
}

func handleProgress(ctx context.Context, progress chan interface{}) {
	go func() {
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

func historicalFuzzyMatching(ctx context.Context, heads hash.HashSet, groupings *nbs.ChunkRelations, db *doltdb.DoltDB) error {
	var hs []hash.Hash
	for h := range heads {
		_, err := db.ReadCommit(ctx, h)
		if err != nil {
			continue
		}
		hs = append(hs, h)
	}

	iterator, err := commitwalk.GetTopologicalOrderIterator(ctx, db, hs, func(cmt *doltdb.OptionalCommit) (bool, error) {
		return true, nil
	})
	if err != nil {
		return err
	}
	for {
		h, _, err := iterator.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		err = relateCommitToParentChunks(ctx, h, groupings, db)
		if err != nil {
			return err
		}
	}

	return nil
}

var ErrNoShallowClones = errors.New("building archives only allowed for full clones")

func relateCommitToParentChunks(ctx context.Context, commit hash.Hash, groupings *nbs.ChunkRelations, db *doltdb.DoltDB) error {
	oCmt, err := db.ReadCommit(ctx, commit)
	if err != nil {
		return nil // Only want commits. Skip others.
	}
	cmt, ok := oCmt.ToCommit()
	if !ok {
		return ErrNoShallowClones
	}
	cmtRv, err := cmt.GetRootValue(ctx)
	if err != nil {
		return err
	}

	// Dolt supports only 1 or 2 parents, but the logic is the same for each. And if there are no parents, no op.
	for i := 0; i < cmt.NumParents(); i++ {
		oCmt, err = cmt.GetParent(ctx, i)
		if err != nil {
			return err
		}
		parent, exists := oCmt.ToCommit()
		if !exists {
			return ErrNoShallowClones
		}

		parentRv, err := parent.GetRootValue(ctx)
		if err != nil {
			return err
		}

		deltas, err := diff.GetTableDeltas(ctx, cmtRv, parentRv)
		if err != nil {
			return err
		}

		for _, delta := range deltas {
			schChg, err := delta.HasSchemaChanged(ctx)
			if err != nil {
				return err
			}
			if schChg {
				continue
			}
			if delta.HasPrimaryKeySetChanged() {
				continue
			}

			changed, err := delta.HasDataChanged(ctx)
			if err != nil {
				return err
			}
			if !changed {
				continue
			}

			from, to, err := delta.GetRowData(ctx)

			f, err := durable.ProllyMapFromIndex(from)
			if err != nil {
				return err
			}
			t, err := durable.ProllyMapFromIndex(to)
			if err != nil {
				return err
			}

			if f.Node().Level() != t.Node().Level() {
				continue
			}
			err = tree.ChunkAddressDiffOrderedTrees(ctx, f.Tuples(), t.Tuples(), func(ctx context.Context, diff tree.AddrDiff) error {
				groupings.Add(diff.From, diff.To)
				return nil
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}
