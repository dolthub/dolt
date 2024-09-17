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

package statspro

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	types2 "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

func (p *Provider) Configure(ctx context.Context, ctxFactory func(ctx context.Context) (*sql.Context, error), bThreads *sql.BackgroundThreads, dbs []dsess.SqlDatabase) error {
	p.SetStarter(NewStatsInitDatabaseHook(p, ctxFactory, bThreads))

	if _, disabled, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsMemoryOnly); disabled == int8(1) {
		return nil
	}

	loadCtx, err := ctxFactory(ctx)
	if err != nil {
		return err
	}

	branches := p.getStatsBranches(loadCtx)

	var autoEnabled bool
	var startupEnabled bool
	var intervalSec time.Duration
	var thresholdf64 float64
	if _, enabled, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsAutoRefreshEnabled); enabled == int8(1) {
		autoEnabled = true
		_, threshold, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsAutoRefreshThreshold)
		_, interval, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsAutoRefreshInterval)
		interval64, _, _ := types2.Int64.Convert(interval)
		intervalSec = time.Second * time.Duration(interval64.(int64))
		thresholdf64 = threshold.(float64)

		p.pro.InitDatabaseHooks = append(p.pro.InitDatabaseHooks, NewStatsInitDatabaseHook(p, ctxFactory, bThreads))
		p.pro.DropDatabaseHooks = append(p.pro.DropDatabaseHooks, NewStatsDropDatabaseHook(p))
	} else if _, startupStats, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsBootstrapEnabled); startupStats == int8(1) {
		startupEnabled = true
	}

	eg, ctx := loadCtx.NewErrgroup()
	for _, db := range dbs {
		// copy closure variables
		db := db
		eg.Go(func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					if str, ok := r.(fmt.Stringer); ok {
						err = fmt.Errorf("%w: %s", ErrFailedToLoad, str.String())
					} else {
						err = fmt.Errorf("%w: %v", ErrFailedToLoad, r)
					}
					return
				}
			}()

			fs, err := p.pro.FileSystemForDatabase(db.Name())
			if err != nil {
				return err
			}

			if p.Load(loadCtx, fs, db, branches); err != nil {
				return err
			}
			if autoEnabled {
				return p.InitAutoRefreshWithParams(ctxFactory, db.Name(), bThreads, intervalSec, thresholdf64, branches)
			} else if startupEnabled {
				if err := p.BootstrapDatabaseStats(loadCtx, db.Name()); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return eg.Wait()
}

// getStatsBranches returns the set of branches whose statistics are tracked.
// The order of precedence is (1) global variable, (2) session current branch,
// (3) engine default branch.
func (p *Provider) getStatsBranches(ctx *sql.Context) []string {
	dSess := dsess.DSessFromSess(ctx.Session)
	var branches []string
	if _, bs, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsBranches); bs == "" {
		defaultBranch, _ := dSess.GetBranch()
		if defaultBranch != "" {
			branches = append(branches, defaultBranch)
		}
	} else {
		for _, branch := range strings.Split(bs.(string), ",") {
			branches = append(branches, strings.TrimSpace(branch))
		}
	}

	if branches == nil {
		branches = append(branches, p.pro.DefaultBranch())
	}
	return branches
}

func (p *Provider) LoadStats(ctx *sql.Context, db, branch string) error {
	if statDb, ok := p.getStatDb(db); ok {
		return statDb.LoadBranchStats(ctx, branch)
	}
	return nil
}

// Load scans the statistics tables, populating the |stats| attribute.
// Statistics are not available for reading until we've finished loading.
func (p *Provider) Load(ctx *sql.Context, fs filesys.Filesys, db dsess.SqlDatabase, branches []string) {
	// |statPath| is either file://./stat or mem://stat
	statsDb, err := p.sf.Init(ctx, db, p.pro, fs, env.GetCurrentUserHomeDir)
	if err != nil {
		ctx.GetLogger().Errorf("initialize stats failure: %s\n", err.Error())
		return
	}

	for _, branch := range branches {
		err = statsDb.LoadBranchStats(ctx, branch)
		if err != nil {
			// if branch name is invalid, continue loading rest
			// TODO: differentiate bad branch name from other errors
			ctx.GetLogger().Errorf("load stats failure: %s\n", err.Error())
			continue
		}
	}

	p.setStatDb(strings.ToLower(db.Name()), statsDb)
	return
}
