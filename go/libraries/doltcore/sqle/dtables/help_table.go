// Copyright 2025 Dolthub, Inc.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// 	http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dtables

import (
	"encoding/json"
	"io"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/go-mysql-server/sql"
	sqlTypes "github.com/dolthub/go-mysql-server/sql/types"
)

type HelpTable struct {
	dbName    string
	tableName string
}

var HelpTableTypes = []string{
	"system_table",
	"procedure",
	"function",
	"variable",
}

// NewHelpTable creates a HelpTable
func NewHelpTable(_ *sql.Context, dbName, tableName string) sql.Table {
	return &HelpTable{dbName: dbName, tableName: tableName}
}

// Name is a sql.Table interface function which returns the name of the table.
func (ht *HelpTable) Name() string {
	return ht.tableName
}

// String is a sql.Table interface function which returns the name of the table.
func (ht *HelpTable) String() string {
	return ht.tableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the help system table.
func (ht *HelpTable) Schema() sql.Schema {
	return []*sql.Column{
		{
			Name:           "name",
			Type:           sqlTypes.TinyText,
			Source:         ht.tableName,
			PrimaryKey:     true,
			DatabaseSource: ht.dbName,
		},
		{
			Name:           "type",
			Type:           sqlTypes.MustCreateEnumType(HelpTableTypes, sql.Collation_Default),
			Source:         ht.tableName,
			PrimaryKey:     false,
			DatabaseSource: ht.dbName,
		},
		{
			Name:           "synopsis",
			Type:           sqlTypes.LongText,
			Source:         ht.tableName,
			PrimaryKey:     false,
			DatabaseSource: ht.dbName,
		},
		{
			Name:           "short_description",
			Type:           sqlTypes.LongText,
			Source:         ht.tableName,
			PrimaryKey:     false,
			DatabaseSource: ht.dbName,
		},
		{
			Name:           "long_description",
			Type:           sqlTypes.LongText,
			Source:         ht.tableName,
			PrimaryKey:     false,
			DatabaseSource: ht.dbName,
		},
		{
			Name:           "arguments",
			Type:           sqlTypes.JSON,
			Source:         ht.tableName,
			PrimaryKey:     false,
			DatabaseSource: ht.dbName,
		},
	}
}

// Collation implements the sql.Table interface.
func (ht *HelpTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition
// of the data. Currently the data is unpartitioned.
func (ht *HelpTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition.
func (ht *HelpTable) PartitionRows(_ *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return NewHelpRowIter(), nil
}

type HelpRowIter []sql.Row

func NewHelpRowIter() *HelpRowIter {
	var nilIter HelpRowIter
	return &nilIter
}

// DoltCommand is set in cmd/dolt/dolt.go to avoid circular dependency.
var DoltCommand cli.SubCommandHandler

func (itr *HelpRowIter) Next(_ *sql.Context) (sql.Row, error) {
	if *itr == nil {
		var err error
		*itr, err = generateProcedureHelpRows(DoltCommand.Name(), DoltCommand.Subcommands)
		if err != nil {
			return nil, err
		}
	}

	helpRows := *itr

	if len(helpRows) == 0 {
		return nil, io.EOF
	}

	row := helpRows[0]
	*itr = helpRows[1:]

	return row, nil
}

func (itr *HelpRowIter) Close(_ *sql.Context) error {
	return nil
}

// generateProcedureHelpRows generates a sql row for each procedure that has an equivalent CLI command.
func generateProcedureHelpRows(cmdStr string, subCommands []cli.Command) ([]sql.Row, error) {
	rows := []sql.Row{}

	for _, curr := range subCommands {
		if hidCmd, ok := curr.(cli.HiddenCommand); ok && hidCmd.Hidden() {
			continue
		}

		if subCmdHandler, ok := curr.(cli.SubCommandHandler); ok {
			if subCmdHandler.Unspecified != nil {
				newRows, err := generateProcedureHelpRows(cmdStr, []cli.Command{subCmdHandler.Unspecified})
				if err != nil {
					return nil, err
				}
				rows = append(rows, newRows...)
			}
			newRows, err := generateProcedureHelpRows(cmdStr+"_"+subCmdHandler.Name(), subCmdHandler.Subcommands)
			if err != nil {
				return nil, err
			}
			rows = append(rows, newRows...)
		} else {
			fullName := cmdStr + "_" + curr.Name()
			procedureName := strings.ReplaceAll(fullName, "-", "_")

			hasProcedure := false
			for _, procedure := range dprocedures.DoltProcedures {
				if procedure.Name == procedureName {
					hasProcedure = true
					break
				}
			}

			docs := curr.Docs()

			if hasProcedure && docs != nil {
				argsMap := map[string]string{}
				for _, usage := range cli.OptionsUsageList(docs.ArgParser, cli.EmptyFormat) {
					argsMap[usage[0]] = usage[1]
				}

				argsJson, err := json.Marshal(argsMap)
				if err != nil {
					return nil, err
				}

				synopsis, err := docs.GetSynopsis(cli.CliFormat)
				if err != nil {
					return nil, err
				}

				synopsisWithCommand := make([]string, len(synopsis))
				cliName := strings.ReplaceAll(fullName, "_", " ")
				for i := range synopsis {
					synopsisWithCommand[i] = cliName + " " + synopsis[i]
				}

				shortDesc := docs.GetShortDesc()

				longDesc, err := docs.GetLongDesc(cli.CliFormat)
				if err != nil {
					return nil, err
				}

				rows = append(rows, sql.NewRow(
					procedureName,
					"procedure",
					strings.Join(synopsisWithCommand, "\n"),
					shortDesc,
					longDesc,
					string(argsJson),
				))
			}
		}
	}

	return rows, nil
}
