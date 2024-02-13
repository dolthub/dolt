package sysbench_runner

import "context"

type Config interface {
	Validate(ctx context.Context) error
	ContainsServerOfType(server ServerType) bool
}
