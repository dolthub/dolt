// Copyright 2023 Dolthub, Inc.
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

package indexcmds

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	val2 "github.com/dolthub/dolt/go/store/val"
	"io"
	"sort"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

const fixupBatchSize = 10

var fixupDocs = &cli.CommandDocumentation{
	CommandStr: "fixup",
	ShortDesc:  `Remove stale secondary index entries`,
	LongDesc: IndexCmdWarning + `
This command removes stale secondary key references. {{.LessThan}}index{{.GreaterThan}}".`,
	Synopsis: []string{
		`[--dry-run | --batch {{.LessThan}}rows (millions){{.GreaterThan}} ] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}index{{.GreaterThan}}`,
		`[--all | --dry-run | --batch {{.LessThan}}rows (millions){{.GreaterThan}}]`,
	},
}

type FixupCmd struct{}

func (cmd FixupCmd) InstallsSignalHandlers() bool {
	return false
}

func (cmd FixupCmd) Name() string {
	return "fixup"
}

func (cmd FixupCmd) Description() string {
	return "Internal debugging command to fixup secondary indexes"
}

func (cmd FixupCmd) Docs() *cli.CommandDocumentation {
	return fixupDocs
}

func (cmd FixupCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "The table that the given index belongs to."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"index", "The name of the index that belongs to the given table."})
	ap.SupportsFlag(cli.AllFlag, "", "Fixup every secondary index")
	ap.SupportsFlag(cli.DryRunFlag, "", "Check for bad secondary index entries. Do not rewrite secondary indexes.")
	ap.SupportsInt(commands.BatchFlag, "b", "batch size (millions)", "Batch primary key lookups")
	return ap
}

func (cmd FixupCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, catDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	all := apr.Contains(cli.AllFlag)
	dry := apr.Contains(cli.DryRunFlag)
	batchSize := apr.GetIntOrDefault(commands.BatchFlag, fixupBatchSize) * 1_000_000

	if apr.NArg() > 0 && all {
		cli.Println("specify table/index or -all, not both")
		usage()
		return 0
	} else if apr.NArg() != 2 && !all {
		cli.Println("specify table/index or -all")
		usage()
		return 0
	}

	working, err := dEnv.WorkingRoot(context.Background())
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to get working.").AddCause(err).Build(), nil)
	}

	if all {
		newRoot, err := cmd.sweepAll(ctx, working, batchSize, dry)
		if err != nil {
			return commands.HandleVErrAndExitCode(errhand.BuildDError("Failed to sweep all").AddCause(err).Build(), nil)
		}
		err = dEnv.UpdateWorkingRoot(ctx, newRoot)
		if err != nil {
			return HandleErr(errhand.BuildDError("Unable to update the working set.").AddCause(err).Build(), nil)
		}

		return 0
	}

	tableName := apr.Arg(0)
	indexName := apr.Arg(1)
	return cmd.sweepSingle(ctx, dEnv, working, tableName, indexName, batchSize, dry)
}

func (cmd FixupCmd) sweepSingle(ctx context.Context, dEnv *env.DoltEnv, working *doltdb.RootValue, tableName, indexName string, batchSize int, dry bool) int {
	table, ok, err := working.GetTable(ctx, tableName)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to get table `%s`.", tableName).AddCause(err).Build(), nil)
	}
	if !ok {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("The table `%s` does not exist.", tableName).Build(), nil)
	}
	tblSch, err := table.GetSchema(ctx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to get schema for `%s`.", tableName).AddCause(err).Build(), nil)
	}
	idx := tblSch.Indexes().GetByName(indexName)
	if idx == nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("The index `%s` does not exist on table `%s`.", indexName, tableName).Build(), nil)
	}
	if isPrimaryKey(idx) {
		return 0
	}

	indexRowData, err := table.GetIndexRowData(ctx, idx.Name())
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Failed to sweep '%s.%s'", tableName, indexName).AddCause(err).Build(), nil)
	}
	updatedTable, err := cmd.sweepIndex(ctx, tableName, indexName, table, indexRowData, batchSize, dry)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Failed to sweep '%s.%s'", tableName, indexName).AddCause(err).Build(), nil)
	}
	if !dry {
		working, err = working.PutTable(ctx, tableName, updatedTable)
		if err != nil {
			return HandleErr(errhand.BuildDError("Unable to set the table for the rebuilt index.").AddCause(err).Build(), nil)
		}
		err = dEnv.UpdateWorkingRoot(ctx, working)
		if err != nil {
			return HandleErr(errhand.BuildDError("Unable to set the root for the rebuilt table.").AddCause(err).Build(), nil)
		}
	}

	return 0
}

func (cmd FixupCmd) sweepAll(ctx context.Context, root *doltdb.RootValue, batchSize int, dry bool) (*doltdb.RootValue, error) {
	schemas, err := root.GetAllSchemas(ctx)
	if err != nil {
		return nil, err
	}
	for tableName, sch := range schemas {
		table, ok, err := root.GetTable(ctx, tableName)
		if !ok {
			return nil, fmt.Errorf("table not found")
		} else if err != nil {
			return nil, err
		}
		for _, idx := range sch.Indexes().AllIndexes() {
			if isPrimaryKey(idx) {
				continue
			}
			rowData, err := table.GetIndexRowData(ctx, idx.Name())
			if err != nil {
				return nil, err
			}
			updatedTable, err := cmd.sweepIndex(ctx, tableName, idx.Name(), table, rowData, batchSize, dry)
			if err != nil {
				return nil, err
			}
			if updatedTable != nil {
				table = updatedTable
			}
		}
		if !dry {
			root, err = root.PutTable(ctx, tableName, table)
			if err != nil {
				return nil, err
			}
		}
	}
	return root, nil
}

type sweepState uint8

const (
	ssUnknown sweepState = iota
	ssFill
	ssFlush
	ssDone
)

type keySet struct {
	pri val2.Tuple
	sec val2.Tuple
}

func (cmd FixupCmd) sweepIndex(ctx context.Context, tableName, indexName string, t *doltdb.Table, idx durable.Index, batchSize int, dry bool) (*doltdb.Table, error) {
	secMap := durable.ProllyMapFromIndex(idx)
	secKd, _ := secMap.Descriptors()
	iter, err := secMap.IterAll(ctx)
	if err != nil {
		return nil, err
	}

	tabSize, err := secMap.Count()
	if err != nil {
		return nil, err
	}
	if tabSize < batchSize {
		batchSize = tabSize
	}

	var mutSec *prolly.MutableMap
	if !dry {
		mutSec = secMap.Mutate()
	}

	rowData, err := t.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	priMap := durable.ProllyMapFromIndex(rowData)
	priKd, _ := priMap.Descriptors()
	priKb := val2.NewTupleBuilder(priKd)
	pool := priMap.Pool()

	keys := make([]keySet, 0, batchSize)
	var key val2.Tuple
	nextState := ssFill
	var cur *tree.Cursor
	var i int

	cli.Printf("starting to fixup '%s.%s'\n", tableName, indexName)

	for {
		select {
		case <-ctx.Done():
			cli.Printf("reached row number '%d'\n", i*batchSize)
			return nil, nil
		default:
		}

		if nextState == ssDone {
			break
		}

		switch nextState {
		case ssFill:
			nextState = ssFlush
			for len(keys) < fixupBatchSize {
				key, _, err = iter.Next(ctx)
				if errors.Is(err, io.EOF) {
					err = nil
					break
				} else if err != nil {
					return nil, err
				}

				if cur == nil {
					cur, err = tree.NewCursorAtKey(ctx, secMap.NodeStore(), secMap.Node(), key, secMap.Tuples().Order)
				} else {
					err = tree.Seek(ctx, cur, key, secKd)
				}
				if !cur.Valid() {
					ok, err := secMap.Has(ctx, key)
					cli.PrintErrf("failed roundtrip: %s; ok: %v; err: %s\n", secKd.Format(key), ok, err)
				}

				o := key.Count() - priKd.Count()
				for i := 0; i < priKd.Count(); i++ {
					j := o + i
					priKb.PutRaw(i, key.GetField(j))
				}
				keys = append(keys, keySet{pri: priKb.Build(pool), sec: key})
			}
		case ssFlush:
			i++
			if len(keys) == 0 {
				nextState = ssDone
				break
			}
			nextState = ssFill
			sort.Slice(keys, func(i, j int) bool {
				return bytes.Compare(keys[i].pri, keys[j].pri) <= 0
			})
			for _, keyPair := range keys {
				//if cur == nil {
				//	cur, err = tree.NewCursorAtKey(ctx, priMap.NodeStore(), priMap.Node(), keyPair.pri, priMap.Tuples().Order)
				//} else {
				//	err = tree.Seek(ctx, cur, keyPair.pri, priKd)
				//}
				ok, err := priMap.Has(ctx, keyPair.pri)
				if err != nil {
					return nil, err
				}

				if !ok {
					// this secondary key references no valid primary key
					cli.PrintErrf("dangling secondary key in '%s.%s': %s -> %s\n", tableName, indexName, secKd.Format(keyPair.sec), priKd.Format(keyPair.pri))
					if dry {
						continue
					}
					err := mutSec.Delete(ctx, keyPair.sec)
					if err != nil {
						return nil, err
					}
				}
			}
			keys = keys[:0]
		case ssDone:
		default:
			panic(fmt.Sprintf("unknown value for sweep state %d", nextState))
		}
	}

	if mutSec != nil {
		res, err := mutSec.Map(ctx)
		if err != nil {
			return nil, err
		}

		return t.SetIndexRows(ctx, indexName, durable.IndexFromProllyMap(res))
	}
	return nil, nil
}

func isPrimaryKey(idx schema.Index) bool {
	if len(idx.PrimaryKeyTags()) != len(idx.IndexedColumnTags()) {
		return false
	}
	pks := idx.PrimaryKeyTags()
	for _, t := range idx.IndexedColumnTags() {
		found := false
		for _, pk := range pks {
			if pk == t {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
