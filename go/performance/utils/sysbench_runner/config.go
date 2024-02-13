package sysbench_runner

import "context"

type Config interface {
	GetRuns() int
	GetScriptDir() string
	GetNomsBinFormat() string
	GetRuntimeOs() string
	GetRuntimeGoArch() string
	GetTestOptions() []string
	GetTestConfigs() []TestConfig

	Validate(ctx context.Context) error
	ContainsServerOfType(server ServerType) bool
}

type TpccConfig interface {
	Config
	GetScaleFactors() []int
}
