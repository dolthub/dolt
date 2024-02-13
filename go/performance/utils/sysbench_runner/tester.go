package sysbench_runner

import "context"

type Tester interface {
	Test(ctx context.Context) (*Result, error)
}

type Test interface {
	GetId() string
	GetName() string
	GetParamsToSlice() []string
	GetPrepareArgs() []string
	GetRunArgs() []string
	GetCleanupArgs() []string
}

type SysbenchTest interface {
	Test
	GetFromScript() bool
}

type TestParams interface {
	ToSlice() []string
}
