// Copyright 2022-2023 Dolthub, Inc.
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
	"fmt"

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
			saveConnID := int(ctx.Session.ID())
			res, err := controller.setRoleAndEpoch(role, epoch, roleTransitionOptions{
				graceful:   true,
				saveConnID: &saveConnID,
			})
			if err != nil {
				// We did not transition, no need to set our session to read-only, etc.
				return nil, err
			}
			if res.changedRole {
				// We transitioned, make sure we do not run anymore queries on this session.
				ctx.Session.SetTransaction(nil)
				dsess.DSessFromSess(ctx.Session).SetValidateErr(ErrServerTransitionedRolesErr)
			}
			return sql.RowsToRowIter(sql.UntypedSqlRow{0}), nil
		},
		ReadOnly: true,
	}
}

func newTransitionToStandbyProcedure(controller *Controller) sql.ExternalStoredProcedureDetails {
	return sql.ExternalStoredProcedureDetails{
		Name: "dolt_cluster_transition_to_standby",
		Schema: sql.Schema{
			&sql.Column{
				Name:     "caught_up",
				Type:     types.Int8,
				Nullable: false,
			},
			&sql.Column{
				Name:     "database",
				Type:     types.LongText,
				Nullable: false,
			},
			&sql.Column{
				Name:     "remote",
				Type:     types.LongText,
				Nullable: false,
			},
			&sql.Column{
				Name:     "remote_url",
				Type:     types.LongText,
				Nullable: false,
			},
		},
		Function: func(ctx *sql.Context, epoch, minCaughtUpStandbys int) (sql.RowIter, error) {
			saveConnID := int(ctx.Session.ID())
			res, err := controller.setRoleAndEpoch("standby", epoch, roleTransitionOptions{
				graceful:            true,
				minCaughtUpStandbys: minCaughtUpStandbys,
				saveConnID:          &saveConnID,
			})
			if err != nil {
				// We did not transition, no need to set our session to read-only, etc.
				return nil, err
			}
			if res.changedRole {
				// We transitioned, make sure we do not run anymore queries on this session.
				ctx.Session.SetTransaction(nil)
				dsess.DSessFromSess(ctx.Session).SetValidateErr(ErrServerTransitionedRolesErr)
				rows := make([]sql.Row, len(res.gracefulTransitionResults))
				for i, r := range res.gracefulTransitionResults {
					var caughtUp int8
					if r.caughtUp {
						caughtUp = 1
					}
					rows[i] = sql.UntypedSqlRow{
						caughtUp,
						r.database,
						r.remote,
						r.remoteUrl,
					}
				}
				return sql.RowsToRowIter(rows...), nil
			} else {
				return nil, fmt.Errorf("failed to transition server to standby; it is already standby.")
			}
		},
	}
}
