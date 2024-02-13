package sysbench_runner

type ServerType string

type ServerConfig interface {
	GetServerExec() string
	GetServerType() ServerType
	GetServerArgs() ([]string, error)
	GetTestingArgs(testConfig TestConfig) []string
	Validate() error
	SetDefaults() error
}

type InitServerConfig interface {
	ServerConfig
	GetInitDbExec() string
}

type ProfilingServerConfig interface {
	ServerConfig
	GetServerProfile() ServerProfile
	GetProfilePath() string
}
