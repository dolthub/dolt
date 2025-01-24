package statspro

import "github.com/dolthub/go-mysql-server/sql"

type StatsNoop struct{}

func (s StatsNoop) GetTableStats(ctx *sql.Context, db string, table sql.Table) ([]sql.Statistic, error) {
	return nil, nil
}

func (s StatsNoop) RefreshTableStats(ctx *sql.Context, table sql.Table, db string) error {
	return nil
}

func (s StatsNoop) SetStats(ctx *sql.Context, stats sql.Statistic) error {
	return nil
}

func (s StatsNoop) GetStats(ctx *sql.Context, qual sql.StatQualifier, cols []string) (sql.Statistic, bool) {
	return nil, false
}

func (s StatsNoop) DropStats(ctx *sql.Context, qual sql.StatQualifier, cols []string) error {
	return nil
}

func (s StatsNoop) DropDbStats(ctx *sql.Context, db string, flush bool) error {
	return nil
}

func (s StatsNoop) RowCount(ctx *sql.Context, db string, table sql.Table) (uint64, error) {
	return 0, nil
}

func (s StatsNoop) DataLength(ctx *sql.Context, db string, table sql.Table) (uint64, error) {
	return 0, nil
}

var _ sql.StatsProvider = StatsNoop{}
