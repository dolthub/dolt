package statspro

import (
	"context"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statsdb"
	"github.com/dolthub/go-mysql-server/sql"
	types2 "github.com/dolthub/go-mysql-server/sql/types"
	"strings"
	"time"
)

func (p *Provider) Configure(ctx context.Context, ctxFactory func(ctx context.Context) (*sql.Context, error), bThreads *sql.BackgroundThreads, pro *sqle.DoltDatabaseProvider, dbs []dsess.SqlDatabase, sf statsdb.StatsFactory) error {
	p.SetStarter(NewInitDatabaseHook(p, ctxFactory, bThreads, nil))

	if _, disabled, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsMemoryOnly); disabled == int8(1) {
		return nil
	}

	loadCtx, err := ctxFactory(ctx)
	if err != nil {
		return err
	}

	dSess := dsess.DSessFromSess(loadCtx.Session)
	var branches []string
	if _, bs, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsMemoryOnly); bs == "" {
		defaultBranch, err := dSess.GetBranch()
		if err != nil {
			return err
		}
		branches = append(branches, defaultBranch)
	} else {
		for _, branch := range strings.Split(bs.(string), ",") {
			branches = append(branches, strings.TrimSpace(branch))
		}
	}

	if err := p.Load(loadCtx, pro, sf, branches); err != nil {
		return err
	}

	if _, enabled, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsAutoRefreshEnabled); enabled == int8(1) {
		_, threshold, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsAutoRefreshThreshold)
		_, interval, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsAutoRefreshInterval)
		interval64, _, _ := types2.Int64.Convert(interval)
		intervalSec := time.Second * time.Duration(interval64.(int64))
		thresholdf64 := threshold.(float64)

		for _, db := range dbs {
			if err := p.InitAutoRefresh(ctxFactory, db.Name(), bThreads, intervalSec, thresholdf64); err != nil {
				return err
			}
		}
		pro.InitDatabaseHook = NewInitDatabaseHook(p, ctxFactory, bThreads, pro.InitDatabaseHook)
		pro.DropDatabaseHook = NewDropDatabaseHook(p, ctxFactory, pro.DropDatabaseHook)
	}
	return nil
}
