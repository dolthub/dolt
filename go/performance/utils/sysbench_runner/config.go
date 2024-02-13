package sysbench_runner

import "context"

type Config interface {
	GetRuns() int
	GetScriptDir() string
	GetNomsBinFormat() string
	GetRuntimeOs() string
	GetRuntimeGoArch() string
	Validate(ctx context.Context) error
	ContainsServerOfType(server ServerType) bool
}

type SysbenchConfig interface {
	Config
	GetTestOptions() []string
	GetTestConfigs() []TestConfig
}

type TpccConfig interface {
	Config
	GetScaleFactors() []int
}
