// Copyright 2022 Dolthub, Inc.
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

package dtables

import (
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

var errUnblameableTable = errors.New("unable to generate blame view for table without primary key")

const (
	viewExpressionTemplate = `
				WITH sorted_diffs_by_pk
				         AS (SELECT
				                 %s,  -- allToPks
				                 to_commit,
				                 to_commit_date,
								 diff_type,
				                 ROW_NUMBER() OVER (
				                     PARTITION BY 
										%s  -- pksPartitionByExpression
				                     ORDER BY 
										coalesce(to_commit_date, from_commit_date) DESC
								) row_num
				             FROM
				                 dolt_diff_%s  -- tableName
				            )
				SELECT
				    %s  -- pksSelectExpression
				    sd.to_commit as commit,
				    sd.to_commit_date as commit_date,
				    dl.committer,
				    dl.email,
				    dl.message
				FROM
				    sorted_diffs_by_pk as sd,
				    dolt_log as dl
				WHERE
				    dl.commit_hash = sd.to_commit
				    and sd.row_num = 1
				    and sd.diff_type <> 'removed'
				ORDER BY 
					%s  -- pksOrderByExpression;
`
)

// NewBlameView returns a view expression for the DOLT_BLAME system view for the specified table.
// The DOLT_BLAME system view is a view on the DOLT_DIFF system table that shows the latest commit
// for each primary key in the specified table.
func NewBlameView(ctx *sql.Context, tableName string, root *doltdb.RootValue) (string, error) {
	table, _, ok, err := root.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", doltdb.ErrTableNotFound
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return "", nil
	}

	blameViewExpression, err := createDoltBlameViewExpression(tableName, sch.GetPKCols().GetColumns())
	if err != nil {
		return "", err
	}

	return blameViewExpression, nil
}

// createDoltBlameViewExpression creates a view expression string to generate the DOLT_BLAME system
// view for the specified table, with the specified primary keys. The DOLT_BLAME system view is built
// from the data in the DOLT_DIFF system table for the same specified table name.
func createDoltBlameViewExpression(tableName string, pks []schema.Column) (string, error) {
	if len(pks) == 0 {
		return "", errUnblameableTable
	}

	allToPks := ""
	pksPartitionByExpression := ""
	pksOrderByExpression := ""
	pksSelectExpression := ""

	for i, pk := range pks {
		if i > 0 {
			allToPks += ", "
			pksPartitionByExpression += ", "
			pksOrderByExpression += ", "
		}

		allToPks += "to_" + pk.Name
		pksPartitionByExpression += "coalesce(to_" + pk.Name + ", from_" + pk.Name + ")"
		pksOrderByExpression += "sd.to_" + pk.Name + " ASC "
		pksSelectExpression += "sd.to_" + pk.Name + " AS " + pk.Name + ", "
	}

	return fmt.Sprintf(viewExpressionTemplate, allToPks, pksPartitionByExpression, tableName,
		pksSelectExpression, pksOrderByExpression), nil
}
