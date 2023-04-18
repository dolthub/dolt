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

package cluster

import (
	"errors"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var ErrServerTransitionedRolesErr = errors.New("this server transitioned cluster roles. this connection can no longer be used. please reconnect.")

func newAssumeRoleProcedure(controller *Controller) sql.ExternalStoredProcedureDetails {
	return sql.ExternalStoredProcedureDetails{
		Name: "dolt_assume_cluster_role",
		Schema: sql.Schema{
			&sql.Column{
				Name:     "status",
				Type:     types.Int64,
				Nullable: false,
			},
		},
		Function: func(ctx *sql.Context, role string, epoch int) (sql.RowIter, error) {
			if role == string(RoleDetectedBrokenConfig) {
				return nil, errors.New("cannot set role to detected_broken_config; valid values are 'primary' and 'standby'")
			}
			changerole, err := controller.setRoleAndEpoch(role, epoch, true /* graceful */, int(ctx.Session.ID()))
			if err != nil {
				// We did not transition, no need to set our session to read-only, etc.
				return nil, err
			}
			if changerole {
				// We transitioned, make sure we do not run anymore queries on this session.
				ctx.Session.SetTransaction(nil)
				dsess.DSessFromSess(ctx.Session).SetValidateErr(ErrServerTransitionedRolesErr)
			}
			return sql.RowsToRowIter(sql.Row{0}), nil
		},
	}
}
