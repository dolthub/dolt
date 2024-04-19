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

type ArchiveCmd struct {
}

func (cmd ArchiveCmd) Name() string {
	return "archive"
}

var docs = cli.CommandDocumentationContent{
	ShortDesc: "Create archive files using native or cgo compression, then verify.",
	LongDesc:  `Run this command on a dolt database only after running 'dolt gc'. This command will create an archive file to the CWD. Suffix: .darc. After the new file is generated, it will read every chunk from the new file and verify that the chunk hashes to the correct addr.`,

	Synopsis: []string{
		`--native | --cgo`,
		`--cgo --swap-dict`,
		`--native --no-group`,
	},
}

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

	// Gotta specify one of these three.
	ap.SupportsFlag("raw", "", "Create an archive file with 0 compression")
	ap.SupportsFlag("cgo", "", "Use cgo for zstd compression")
	ap.SupportsFlag("native", "", "Use klauspost/compress for zstd compression")

	ap.SupportsFlag("no-grouping", "", "Do not attempt to group chunks or use dictionaries.")
	ap.SupportsFlag("swap-dict", "", "If using --cgo, generate the dictionary using native. Opposite for --native")

	return ap
}
func (cmd ArchiveCmd) Hidden() bool {
	return true
}

func (cmd ArchiveCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, docs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	db := doltdb.HackDatasDatabaseFromDoltDB(dEnv.DoltDB)
	cs := datas.ChunkStoreFromDatabase(db)
	if _, ok := cs.(*nbs.GenerationalNBS); !ok {
		cli.PrintErrln("archive command requires a GenerationalNBS")
	}

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

	v := dEnv.DoltDB

	groupings := nbs.NewChunkRelations()

	err = historicalFuzzyMatching(ctx, hs, &groupings, v)
	if err != nil {
		cli.PrintErrln(err)
		return 1
	}

	cli.Printf("Found %d possible relations by walking history\n", groupings.Count())

	cfg := nbs.ExpConfig{
		Group: true,
	}
	if apr.Contains("raw") {
		cfg.Cmp = false // noop
	} else if apr.Contains("cgo") {
		cfg.Cmp = true
	} else if apr.Contains("native") {
		cfg.Cmp = true
		cfg.NativeEncoder = true
		cfg.NativeDict = true
	} else {
		cli.PrintErrln("Must specify one of --raw, --cgo, or --native")
		return 1
	}

	if apr.Contains("swap-dict") {
		cfg.NativeDict = !cfg.NativeDict
	}

	if apr.Contains("no-grouping") {
		cfg.Group = false
	}

	err = nbs.RunExperiment(cs, &groupings, cfg, func(format string, args ...interface{}) {
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
		return nil // Only want commits. Skip others.
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
