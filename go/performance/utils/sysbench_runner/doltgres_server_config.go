package sysbench_runner

import (
	"fmt"
	"github.com/google/uuid"
)

type doltgresServerConfigImpl struct {
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
}

var _ ServerConfig = &doltgresServerConfigImpl{}

func NewDoltgresServerConfig(version, serverExec, serverUser, host, resultsFormat string, port int, serverArgs []string) *doltgresServerConfigImpl {
	return &doltgresServerConfigImpl{
		Id:            uuid.New().String(),
		Host:          host,
		Port:          port,
		Version:       version,
		ResultsFormat: resultsFormat,
		ServerExec:    serverExec,
		ServerUser:    serverUser,
		ServerArgs:    serverArgs,
	}
}

func (sc *doltgresServerConfigImpl) GetServerType() ServerType {
	return Doltgres
}

func (sc *doltgresServerConfigImpl) GetServerExec() string {
	return sc.ServerExec
}

func (sc *doltgresServerConfigImpl) GetServerArgs() ([]string, error) {
	params := make([]string, 0)
	if sc.Host != "" {
		params = append(params, fmt.Sprintf("%s=%s", hostFlag, sc.Host))
	}
	if sc.Port != 0 {
		params = append(params, fmt.Sprintf("%s=%d", portFlag, sc.Port))
	}
	params = append(params, sc.ServerArgs...)
	return params, nil
}

func (sc *doltgresServerConfigImpl) GetTestingArgs(testConfig TestConfig) []string {
	params := make([]string, 0)
	params = append(params, defaultSysbenchParams...)
	params = append(params, "--db-driver=pgsql") // todo: replace with consts
	params = append(params, fmt.Sprintf("--pgsql-db=%s", dbName))
	params = append(params, fmt.Sprintf("--pgsql-host=%s", sc.Host))
	params = append(params, "--pgsql-user=doltgres")
	if sc.Port != 0 {
		params = append(params, fmt.Sprintf("--pgsql-port=%d", sc.Port))
	}
	params = append(params, testConfig.GetOptions()...)
	params = append(params, testConfig.GetName())
	return params
}

func (sc *doltgresServerConfigImpl) Validate() error {
	if sc.Version == "" {
		return getMustSupplyError("version")
	}
	if sc.ResultsFormat == "" {
		return getMustSupplyError("results format")
	}
	if sc.ServerExec == "" {
		return getMustSupplyError("server exec")
	}
	return CheckExec(sc.ServerExec, "server exec")
}

func (sc *doltgresServerConfigImpl) SetDefaults() error {
	if sc.Host == "" {
		sc.Host = defaultHost
	}
	if sc.Port < 1 {
		sc.Port = defaultDoltgresPort
	}
	return nil
}

func (sc *doltgresServerConfigImpl) GetId() string {
	return sc.Id
}

func (sc *doltgresServerConfigImpl) GetHost() string {
	return sc.Host
}

func (sc *doltgresServerConfigImpl) GetPort() int {
	return sc.Port
}

func (sc *doltgresServerConfigImpl) GetVersion() string {
	return sc.Version
}
