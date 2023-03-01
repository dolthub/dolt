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
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/store/types"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/val"
)

var verifyDocs = &cli.CommandDocumentation{
	CommandStr: "verify",
	ShortDesc:  `Detect out of order secondary index entries`,
	LongDesc: IndexCmdWarning + `
This command detects out of order secondary key references. {{.LessThan}}index{{.GreaterThan}}".`,
	Synopsis: []string{
		`{{.LessThan}}table{{.GreaterThan}} {{.LessThan}}index{{.GreaterThan}}`,
		`[--all]`,
	},
}

type VerifyCmd struct{}

func (cmd VerifyCmd) InstallsSignalHandlers() bool {
	return false
}

func (cmd VerifyCmd) Name() string {
	return "verify"
}

func (cmd VerifyCmd) Description() string {
	return "Internal debugging command to identify unordered secondary indexes"
}

func (cmd VerifyCmd) Docs() *cli.CommandDocumentation {
	return fixupDocs
}

func (cmd VerifyCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "The table that the given index belongs to."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"index", "The name of the index that belongs to the given table."})
	ap.SupportsFlag(cli.AllFlag, "", "Verify every secondary index")
	return ap
}

func (cmd VerifyCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(verifyDocs)
	apr := cli.ParseArgsOrDie(ap, args, help)

	all := apr.Contains(cli.AllFlag)

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
		err := cmd.verifyAll(ctx, working)
		if err != nil {
			return commands.HandleVErrAndExitCode(errhand.BuildDError("Failed to sweep all").AddCause(err).Build(), nil)
		}
		return 0
	}

	tableName := apr.Arg(0)
	indexName := apr.Arg(1)
	return cmd.verifyOne(ctx, dEnv, working, tableName, indexName)
}

func (cmd VerifyCmd) verifyOne(ctx context.Context, dEnv *env.DoltEnv, working *doltdb.RootValue, tableName, indexName string) int {
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

	indexRowData, err := table.GetIndexRowData(ctx, idx.Name())
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Failed to sweep '%s.%s'", tableName, indexName).AddCause(err).Build(), nil)
	}

	err = cmd.verifyIndex(ctx, tableName, indexName, indexRowData)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Failed to sweep '%s.%s'", tableName, indexName).AddCause(err).Build(), nil)
	}

	return 0
}

func (cmd VerifyCmd) verifyAll(ctx context.Context, root *doltdb.RootValue) error {
	schemas, err := root.GetAllSchemas(ctx)
	if err != nil {
		return err
	}
	for tableName, sch := range schemas {
		table, ok, err := root.GetTable(ctx, tableName)
		if !ok {
			return fmt.Errorf("table not found")
		} else if err != nil {
			return err
		}
		for _, idx := range sch.Indexes().AllIndexes() {
			rowData, err := table.GetIndexRowData(ctx, idx.Name())
			if err != nil {
				return err
			}
			err = cmd.verifyIndex(ctx, tableName, idx.Name(), rowData)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (cmd VerifyCmd) verifyIndex(ctx context.Context, tableName, indexName string, idx durable.Index) error {
	if types.IsFormat_DOLT(idx.Format()) {
		return cmd.verifyDoltIndex(ctx, tableName, indexName, idx)
	}
	return cmd.verifyNomsIndex(ctx, tableName, indexName, idx)
}

func (cmd VerifyCmd) verifyDoltIndex(ctx context.Context, tableName, indexName string, idx durable.Index) error {
	secMap := durable.ProllyMapFromIndex(idx)
	secKd, _ := secMap.Descriptors()
	iter, err := secMap.IterAll(ctx)
	if err != nil {
		return err
	}

	var curr, prev val.Tuple
	for {
		curr, _, err = iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		// check for out of order keys
		if prev != nil && secKd.Compare(curr, prev) <= 0 {
			cli.PrintErrf("'%s.%s' entry is out of order: (%s <= %s)\n",
				tableName, indexName, secKd.Format(curr), secKd.Format(prev))
		}
		prev = curr
	}
}

func (cmd VerifyCmd) verifyNomsIndex(ctx context.Context, tableName, indexName string, idx durable.Index) error {
	secMap := durable.NomsMapFromIndex(idx)
	nbf := secMap.Format()

	var prev types.Value
	return secMap.Iter(ctx, func(curr, _ types.Value) (stop bool, err error) {
		if prev != nil {
			var less bool
			if less, err = prev.Less(nbf, curr); err != nil {
				return
			} else if !less {
				cli.PrintErrf("'%s.%s' entry is out of order: (%s <= %s)\n",
					tableName, indexName, curr.HumanReadableString(), prev.HumanReadableString())
			}
		}
		prev = curr
		return
	})
}
