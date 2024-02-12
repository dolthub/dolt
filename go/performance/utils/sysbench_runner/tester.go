package sysbench_runner

import "context"

type Tester interface {
	Test(ctx context.Context) (*Result, error)
}
