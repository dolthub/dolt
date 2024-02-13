package sysbench_runner

import (
	"fmt"
	"github.com/google/uuid"
	"os"
)

const (
	CpuServerProfile = "cpu"
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

func (sc *doltServerConfigImpl) GetProfilePath() string {
	return sc.ProfilePath
}

func (sc *doltServerConfigImpl) GetServerProfile() ServerProfile {
	return sc.ServerProfile
}

func (sc *doltServerConfigImpl) GetServerType() ServerType {
	return Dolt
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

func (sc *doltServerConfigImpl) GetTestingArgs(testConfig TestConfig) []string {
	params := make([]string, 0)
	params = append(params, defaultSysbenchParams...)
	params = append(params, fmt.Sprintf("--mysql-db=%s", dbName))
	params = append(params, "--db-driver=mysql")
	params = append(params, fmt.Sprintf("--mysql-host=%s", sc.Host))
	params = append(params, "--mysql-user=root")
	if sc.Port != 0 {
		params = append(params, fmt.Sprintf("--mysql-port=%d", sc.Port))
	}
	params = append(params, testConfig.GetOptions()...)
	params = append(params, testConfig.GetName())
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
