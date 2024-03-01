package statspro

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	types2 "github.com/dolthub/go-mysql-server/sql/types"
	"strings"
	"time"
)

func (p *Provider) Configure(ctx context.Context, ctxFactory func(ctx context.Context) (*sql.Context, error), bThreads *sql.BackgroundThreads, pro *sqle.DoltDatabaseProvider, dbs []dsess.SqlDatabase, sf StatsFactory) error {
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

// Load scans the statistics tables, populating the |stats| attribute.
// Statistics are not available for reading until we've finished loading.
func (p *Provider) Load(ctx *sql.Context, pro *sqle.DoltDatabaseProvider, sf StatsFactory, branches []string) error {
	//for _, db := range pro.DoltDatabases() {
	//	// set map keys so concurrent orthogonal writes are OK
	//	p.setStats(strings.ToLower(db.Name()), newDbStats(strings.ToLower(db.Name())))
	//}

	eg, ctx := ctx.NewErrgroup()
	for _, db := range pro.DoltDatabases() {
		// copy closure variables
		dbName := strings.ToLower(db.Name())
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

			fs, err := pro.FileSystemForDatabase(db.Name())

			// get or create reference to stats db
			statsDb, err := sf.Init(ctx, fs, env.GetCurrentUserHomeDir)
			if err != nil {
				ctx.Warn(0, err.Error())
				return nil
			}

			for _, branch := range branches {
				err = statsDb.Load(nil, branch)
				if err != nil {
					ctx.Warn(0, err.Error())
					continue
				}
			}

			p.statDbs[dbName] = statsDb

			//m, err := db.DbData().Ddb.GetStatistics(ctx)
			//if errors.Is(err, doltdb.ErrNoStatistics) {
			//	return nil
			//} else if err != nil {
			//	return err
			//}
			//stats, err := loadStats(ctx, db, statsDb)
			//if errors.Is(err, dtables.ErrIncompatibleVersion) {
			//	ctx.Warn(0, err.Error())
			//	return nil
			//} else if err != nil {
			//	return err
			//}
			//p.setStats(dbName, stats)
			return nil
		})
	}
	return eg.Wait()
}
