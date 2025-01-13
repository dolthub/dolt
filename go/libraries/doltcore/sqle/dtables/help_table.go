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
	"fmt"
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
			Name:           "target",
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

type HelpRowIter struct {
	idx  *int
	rows *[]sql.Row
}

func NewHelpRowIter() HelpRowIter {
	idx := 0
	var nilRows []sql.Row
	return HelpRowIter{idx: &idx, rows: &nilRows}
}

// DoltCommand is set in cmd/dolt/dolt.go to avoid circular dependency.
var DoltCommand cli.SubCommandHandler

func (itr HelpRowIter) Next(_ *sql.Context) (sql.Row, error) {
	if *itr.rows == nil {
		var err error
		*itr.rows, err = generateProcedureHelpRows(DoltCommand.Name(), DoltCommand.Subcommands)
		if err != nil {
			return nil, err
		}
	}

	helpRows := *itr.rows

	if *itr.idx >= len(helpRows) {
		return nil, io.EOF
	}

	row := helpRows[*itr.idx]
	(*itr.idx)++
	return row, nil
}

func (itr HelpRowIter) Close(_ *sql.Context) error {
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
			nameFormatted := fmt.Sprintf("%s_%s", cmdStr, curr.Name())

			nameComparable := strings.ReplaceAll(nameFormatted, "-", "_")

			hasProcedure := false
			for _, procedure := range dprocedures.DoltProcedures {
				if procedure.Name == nameComparable {
					hasProcedure = true
					break
				}
			}

			docs := curr.Docs()

			if hasProcedure && docs != nil {
				argsMap := map[string]string{}
				for _, argHelp := range curr.Docs().ArgParser.ArgListHelp {
					argsMap[argHelp[0]] = argHelp[1]
				}

				argsJson, err := json.Marshal(argsMap)
				if err != nil {
					return nil, err
				}

				rows = append(rows, sql.NewRow(
					nameFormatted,
					"procedure",
					curr.Docs().ShortDesc,
					curr.Docs().LongDesc,
					argsJson,
				))
			}
		}
	}

	return rows, nil
}
