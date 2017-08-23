package config

type SwarmConfig struct {
	AddrFilters             []string
	DisableBandwidthMetrics bool
	DisableNatPortMap       bool
}
