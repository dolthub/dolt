// Copyright 2021 Dolthub, Inc.
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

package schcmds

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	commands "github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

type ChangeTypeCmd struct{}

func (cmd ChangeTypeCmd) Hidden() bool {
	return true
}

func (cmd ChangeTypeCmd) Name() string {
	return "change-type"
}

func (cmd ChangeTypeCmd) Description() string {
	return "Changes the type of a column in place"
}

func (cmd ChangeTypeCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, tblSchemaDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() < 3 {
		cli.PrintUsage("change-type <table> <column> <type>",
			[]string{"Changes the type of a column in place, without modifying any row data.\n" +
				"This is an unsafe operation in general, but widening a type is safe.\n" +
				"Only VARCHAR and INTEGER types are currently supported.\n"},
			ap)
		return 1
	}

	tableName, column, typ := apr.Arg(0), apr.Arg(1), apr.Arg(2)

	root, verr := commands.GetWorkingWithVErr(dEnv)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	table, tableCase, ok, err := root.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("unable to get table '%s'", tableName).AddCause(err).Build(), usage)
	} else if !ok {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("couldn't find table '%s'", tableName).Build(), usage)
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("unable to get table '%s'", tableName).AddCause(err).Build(), usage)
	}

	cols := make([]schema.Column, sch.GetAllCols().Size())
	i := 0
	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if strings.ToLower(col.Name) == strings.ToLower(column) {
			cols[i] = alterColumnType(typ, col)
		} else {
			cols[i] = col
		}
		i++

		return false, nil
	})

	collection, err := schema.NewColCollection(cols...)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("unable to create new schema '%s'", tableName).AddCause(err).Build(), usage)
	}

	newSch, err := schema.SchemaFromCols(collection)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("unable to create new schema '%s'", tableName).AddCause(err).Build(), usage)
	}

	newTable, err := table.UpdateSchema(ctx, newSch)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("unable to create new schema '%s'", tableName).AddCause(err).Build(), usage)
	}

	root, err = root.PutTable(ctx, tableCase, newTable)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("unable to write new table '%s'", tableName).AddCause(err).Build(), usage)
	}

	return commands.HandleVErrAndExitCode(commands.UpdateWorkingWithVErr(dEnv, root), usage)
}

func alterColumnType(typ string, col schema.Column) schema.Column {
	nc := col
	typ = strings.ToLower(typ)
	switch true {
	// TODO: support for other types, nullability
	case strings.HasPrefix(typ, "varchar"):
		lengthStr := typ[len("varchar")+1 : len(typ)-1]
		length, err := strconv.ParseInt(lengthStr, 10, 64)
		if err != nil {
			panic(err)
		}

		ti := col.TypeInfo.ToSqlType().(sql.StringType)
		sqlType := sql.MustCreateString(sqltypes.VarChar, length, ti.Collation())
		nc.TypeInfo, err = typeinfo.FromSqlType(sqlType)
		if err != nil {
			panic(err)
		}
	case typ == "tinyint":
		nc.TypeInfo = typeinfo.Int8Type
	case typ == "smallint":
		nc.TypeInfo = typeinfo.Int16Type
	case typ == "mediumint":
		nc.TypeInfo = typeinfo.Int24Type
	case typ == "int", typ == "integer":
		nc.TypeInfo = typeinfo.Int32Type
	case typ == "bigint":
		nc.TypeInfo = typeinfo.Int64Type
	case typ == "tinyint unsigned":
		nc.TypeInfo = typeinfo.Uint8Type
	case typ == "smallint unsigned":
		nc.TypeInfo = typeinfo.Uint16Type
	case typ == "mediumint unsigned":
		nc.TypeInfo = typeinfo.Uint24Type
	case typ == "int unsigned", typ == "integer unsigned":
		nc.TypeInfo = typeinfo.Uint32Type
	case typ == "bigint unsigned":
		nc.TypeInfo = typeinfo.Uint64Type
	default:
		panic(fmt.Sprintf("unsupported type %s", typ))
	}

	return nc
}

func (cmd ChangeTypeCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "table(s) whose schema is being changed"})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"column", "column whose type is being changed"})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"type", "new column type as a SQL type string"})
	return ap
}

func (cmd ChangeTypeCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	return nil
}

var _ cli.Command = ChangeTypeCmd{}
