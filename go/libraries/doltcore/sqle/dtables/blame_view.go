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
				                 %s,
				                 to_commit,
				                 to_commit_date,
				                 ROW_NUMBER() OVER (
				                     PARTITION BY %s
				                     ORDER BY to_commit_date DESC) row_num
				             FROM
				                 dolt_diff_%s 
				             WHERE %s
				            )
				SELECT
				    %s
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
				ORDER BY %s;
`
)

// NewBlameView returns a view expression for the DOLT_BLAME system view for the specified table.
// The DOLT_BLAME system view is a view on the DOLT_DIFF system table that shows the latest commit
// for each primary key in the specified table.
func NewBlameView(ctx *sql.Context, tableName string, root *doltdb.RootValue) (string, error) {
	ss, ok, err := root.GetSuperSchema(ctx, tableName)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", doltdb.ErrTableNotFound
	}

	sch, err := ss.GenerateSchema()
	if err != nil {
		return "", err
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
	pksNotNullExpression := ""
	pksOrderByExpression := ""
	pksSelectExpression := ""

	for i, pk := range pks {
		if i > 0 {
			allToPks += ", "
			pksNotNullExpression += " AND "
			pksOrderByExpression += ", "
		}

		allToPks += "to_" + pk.Name
		pksNotNullExpression += "to_" + pk.Name + " IS NOT NULL "
		pksOrderByExpression += "sd.to_" + pk.Name + " ASC "
		pksSelectExpression += "sd.to_" + pk.Name + " AS " + pk.Name + ", "
	}

	return fmt.Sprintf(viewExpressionTemplate, allToPks, allToPks, tableName,
		pksNotNullExpression, pksSelectExpression, pksOrderByExpression), nil
}
