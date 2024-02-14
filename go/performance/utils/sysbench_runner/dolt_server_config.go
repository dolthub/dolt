package sysbench_runner

import (
	"fmt"
	"os"

	"github.com/google/uuid"
)

type ServerProfile string

type doltServerConfigImpl struct {
	// Id is a unique id for this servers benchmarking
	Id string

	// Host is the server host
	Host string

	// Port is the server port
	Port int

	// Version is the server version
	Version string

	// ResultsFormat is the format the results should be written in
	ResultsFormat string

	// ServerExec is the path to a server executable
	ServerExec string

	// ServerUser is the user account that should start the server
	ServerUser string

	// ServerArgs are the args used to start a server
	ServerArgs []string

	// ServerProfile specifies the golang profile to take of a Dolt server
	ServerProfile ServerProfile

	// ProfilePath path to directory where server profile will be written
	ProfilePath string
}

var _ ProfilingServerConfig = &doltServerConfigImpl{}

func NewDoltServerConfig(version, serverExec, serverUser, host, resultsFormat, profilePath string, serverProfile ServerProfile, port int, serverArgs []string) *doltServerConfigImpl {
	return &doltServerConfigImpl{
		Id:            uuid.New().String(),
		Host:          host,
		Port:          port,
		Version:       version,
		ResultsFormat: resultsFormat,
		ServerExec:    serverExec,
		ServerUser:    serverUser,
		ServerArgs:    serverArgs,
		ServerProfile: serverProfile,
		ProfilePath:   profilePath,
	}
}

func (sc *doltServerConfigImpl) GetId() string {
	return sc.Id
}

func (sc *doltServerConfigImpl) GetHost() string {
	return sc.Host
}

func (sc *doltServerConfigImpl) GetPort() int {
	return sc.Port
}

func (sc *doltServerConfigImpl) GetVersion() string {
	return sc.Version
}

func (sc *doltServerConfigImpl) GetProfilePath() string {
	return sc.ProfilePath
}

func (sc *doltServerConfigImpl) GetServerProfile() ServerProfile {
	return sc.ServerProfile
}

func (sc *doltServerConfigImpl) GetServerType() ServerType {
	return Dolt
}

func (sc *doltServerConfigImpl) GetResultsFormat() string {
	return sc.ResultsFormat
}

func (sc *doltServerConfigImpl) GetServerExec() string {
	return sc.ServerExec
}

// GetServerArgs returns the args used to start a server
func (sc *doltServerConfigImpl) GetServerArgs() ([]string, error) {
	params := make([]string, 0)
	params = append(params, defaultDoltServerParams...)
	if sc.Host != "" {
		params = append(params, fmt.Sprintf("%s=%s", hostFlag, sc.Host))
	}
	if sc.Port != 0 {
		params = append(params, fmt.Sprintf("%s=%d", portFlag, sc.Port))
	}
	params = append(params, sc.ServerArgs...)
	return params, nil
}

func (sc *doltServerConfigImpl) GetTestingParams(testConfig TestConfig) TestParams {
	params := NewSysbenchTestParams()
	params.Append(defaultSysbenchParams...)
	params.Append(fmt.Sprintf("%s=%s", sysbenchMysqlDbFlag, dbName))
	params.Append(fmt.Sprintf("%s=%s", sysbenchDbDriverFlag, mysqlDriverName))
	params.Append(fmt.Sprintf("%s=%s", sysbenchMysqlHostFlag, sc.Host))
	params.Append(fmt.Sprintf("%s=%s", sysbenchMysqlUserFlag, defaultMysqlUser))
	if sc.Port != 0 {
		params.Append(fmt.Sprintf("%s=%d", sysbenchMysqlPortFlag, sc.Port))
	}
	params.Append(testConfig.GetOptions()...)
	params.Append(testConfig.GetName())
	return params
}

func (sc *doltServerConfigImpl) Validate() error {
	if sc.Version == "" {
		return getMustSupplyError("version")
	}
	if sc.ResultsFormat == "" {
		return getMustSupplyError("results format")
	}
	if sc.ServerExec == "" {
		return getMustSupplyError("server exec")
	}
	if sc.ServerProfile != "" {
		if sc.ServerProfile != CpuServerProfile {
			return fmt.Errorf("unsupported server profile: %s", sc.ServerProfile)
		}
	}
	return CheckExec(sc.ServerExec, "server exec")
}

func (sc *doltServerConfigImpl) SetDefaults() error {
	if sc.Host == "" {
		sc.Host = defaultHost
	}
	if sc.Port < 1 {
		sc.Port = defaultDoltPort
	}
	if sc.ServerProfile != "" {
		if sc.ProfilePath == "" {
			cwd, err := os.Getwd()
			if err != nil {
				return err
			}
			sc.ProfilePath = cwd
		}
	}
	return nil
}
