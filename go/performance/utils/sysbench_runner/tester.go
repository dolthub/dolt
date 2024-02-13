package sysbench_runner

import "context"

type Tester interface {
	Test(ctx context.Context) (*Result, error)
}

type Test interface {
	PrepareArgs() []string
	RunArgs() []string
	CleanupArgs() []string
}
