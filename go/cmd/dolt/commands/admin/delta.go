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
	"io"

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

type DeltaCmd struct {
}

func (cmd DeltaCmd) Name() string {
	return "delta"
}

// Description returns a description of the command
func (cmd DeltaCmd) Description() string {
	return "Hidden command to kick the tires with the new archive format."
}
func (cmd DeltaCmd) RequiresRepo() bool {
	return true
}
func (cmd DeltaCmd) Docs() *cli.CommandDocumentation {
	return nil
}

func (cmd DeltaCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	return ap
}
func (cmd DeltaCmd) Hidden() bool {
	return true
}

func (cmd DeltaCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	db := doltdb.HackDatasDatabaseFromDoltDB(dEnv.DoltDB)
	cs := datas.ChunkStoreFromDatabase(db)
	if _, ok := cs.(*nbs.GenerationalNBS); !ok {
		cli.PrintErrln("Delta command requires a GenerationalNBS")
	}

	datasets, err := db.Datasets(ctx)
	if err != nil {
		cli.PrintErrln(err)
		return 1
	}

	hs := hash.NewHashSet()
	err = datasets.IterAll(ctx, func(id string, hash hash.Hash) error {
		// NM4 - filter out workingSets
		// cli.Printf("Dataset: %s -> %s\n", id, hash.String())
		hs.Insert(hash)
		return nil
	})

	v := dEnv.DoltDB

	groupings := nbs.NewChunkRelations()

	err = historicalFuzzyMatching(ctx, hs, &groupings, v)
	if err != nil {
		cli.PrintErrln(err)
		return 1
	}

	cli.Printf("Found %d possible relations by walking history\n", groupings.Count())

	err = nbs.RunExperiment(cs, &groupings, func(format string, args ...interface{}) {
		cli.Printf(format, args...)
	})
	if err != nil {
		cli.PrintErrln(err)
		return 1
	}

	return 0
}

func historicalFuzzyMatching(ctx context.Context, heads hash.HashSet, groupings *nbs.ChunkRelations, db *doltdb.DoltDB) error {
	hs := []hash.Hash{}
	for h := range heads {
		_, err := db.ReadCommit(ctx, h)
		if err != nil {
			continue // NM4 - we should filter these before they get here
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

		err = diffCommits(ctx, h, groupings, db)
		if err != nil {
			return err
		}
	}

	return nil
}

func diffCommits(ctx context.Context, h hash.Hash, groupings *nbs.ChunkRelations, db *doltdb.DoltDB) error {
	oCmt, err := db.ReadCommit(ctx, h)
	if err != nil {
		return nil // NM4 - we should filter these before they get here
	}
	cmtA, ok := oCmt.ToCommit()
	if !ok {
		return fmt.Errorf("Ghost commit")
	}

	if cmtA.NumParents() == 0 {
		return nil
	}
	oCmt, err = cmtA.GetParent(ctx, 0) // NM4 - Follow the merge!
	cmtB, ok := oCmt.ToCommit()
	if !ok {
		return fmt.Errorf("Ghost commit")
	}

	rvA, err := cmtA.GetRootValue(ctx)
	if err != nil {
		return err
	}
	rvB, err := cmtB.GetRootValue(ctx)
	if err != nil {
		return err
	}

	deltas, err := diff.GetTableDeltas(ctx, rvA, rvB)
	if err != nil {
		return err
	}

	for _, delta := range deltas {
		schChg, err := delta.HasSchemaChanged(ctx)
		if err != nil {
			return err
		}
		if schChg {
			// cli.PrintErrln("Schema changed. Skipping.")
			continue
		}
		fkChg := delta.HasFKChanges()
		if fkChg {
			// cli.PrintErrln("FKs changed. Skipping.")
			continue
		}
		pkChg := delta.HasPrimaryKeySetChanged()
		if pkChg {
			// cli.PrintErrln("PK changed. Skipping.")
			continue
		}

		changed, err := delta.HasDataChanged(ctx)
		if err != nil {
			return err
		}
		if !changed {
			cli.PrintErrln("No data changes. Skipping")
			continue
		}

		// cli.Printf("---------------------- Table ---------------------: %s\n", delta.FromName)

		from, to, err := delta.GetRowData(ctx)

		f := durable.ProllyMapFromIndex(from)
		t := durable.ProllyMapFromIndex(to)

		err = tree.ChunkAddressDiffOrderedTrees(ctx, f.Tuples(), t.Tuples(), func(ctx context.Context, diff tree.Diff) error {
			if diff.Type == tree.ModifiedDiff {
				// cli.Printf("Possible Relation: (%s <-> %s)\n", hash.Hash(diff.From).String(), hash.Hash(diff.To).String())

				groupings.Add(hash.Hash(diff.From), hash.Hash(diff.To))
			}
			return nil
		})
		if err != nil && err != io.EOF && err != tree.ErrShallowTree {
			return err
		}
	}

	return nil
}
