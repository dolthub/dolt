package sysbench_runner

import "context"

type Tester interface {
	Test(ctx context.Context) (*Result, error)
}

type Test interface {
	GetId() string
	GetName() string
	GetParamsToSlice() []string
	GetPrepareArgs(serverConfig ServerConfig) []string
	GetRunArgs(serverConfig ServerConfig) []string
	GetCleanupArgs(serverConfig ServerConfig) []string
}

type SysbenchTest interface {
	Test
	GetFromScript() bool
}

type TestParams interface {
	ToSlice() []string
}

type TpccTestParams interface {
	TestParams
	GetNumThreads() int
	GetScaleFactor() int
	GetTables() int
	GetTrxLevel() string
	GetReportCSV() bool
	GetReportInterval() int
	GetTime() int
}
