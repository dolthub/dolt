package sysbench_runner

import "context"

type Benchmarker interface {
	Benchmark(ctx context.Context) (Results, error)
}
