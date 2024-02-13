package sysbench_runner

import (
	"fmt"
	"github.com/google/uuid"
)

type postgresServerConfigImpl struct {
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

	// InitExec is the path to the server init db executable
	InitExec string

	// ServerUser is the user account that should start the server
	ServerUser string

	// ServerArgs are the args used to start a server
	ServerArgs []string
}

var _ InitServerConfig = &postgresServerConfigImpl{}

func NewPostgresServerConfig(version, serverExec, initDbExec, serverUser, host, resultsFormat string, port int, serverArgs []string) *postgresServerConfigImpl {
	return &postgresServerConfigImpl{
		Id:            uuid.New().String(),
		Host:          host,
		Port:          port,
		Version:       version,
		ResultsFormat: resultsFormat,
		ServerExec:    serverExec,
		InitExec:      initDbExec,
		ServerUser:    serverUser,
		ServerArgs:    serverArgs,
	}
}

func (sc *postgresServerConfigImpl) GetServerExec() string {
	return sc.ServerExec
}

func (sc *postgresServerConfigImpl) GetInitDbExec() string {
	return sc.InitExec
}

func (sc *postgresServerConfigImpl) GetServerType() ServerType {
	return Postgres
}

func (sc *postgresServerConfigImpl) GetServerArgs() ([]string, error) {
	params := make([]string, 0)
	if sc.Port != 0 {
		params = append(params, fmt.Sprintf("%s=%d", portFlag, sc.Port))
	}
	params = append(params, sc.ServerArgs...)
	return params, nil
}

func (sc *postgresServerConfigImpl) GetTestingArgs(testConfig TestConfig) []string {
	params := make([]string, 0)
	params = append(params, defaultSysbenchParams...)
	params = append(params, "--db-driver=pgsql")
	params = append(params, fmt.Sprintf("--pgsql-db=%s", dbName))
	params = append(params, fmt.Sprintf("--pgsql-host=%s", sc.Host))
	params = append(params, "--pgsql-user=postgres")
	if sc.Port != 0 {
		params = append(params, fmt.Sprintf("--pgsql-port=%d", sc.Port))
	}
	params = append(params, testConfig.GetOptions()...)
	params = append(params, testConfig.GetName())
	return params
}

func (sc *postgresServerConfigImpl) Validate() error {
	if sc.Version == "" {
		return getMustSupplyError("version")
	}
	if sc.ResultsFormat == "" {
		return getMustSupplyError("results format")
	}
	if sc.ServerExec == "" {
		return getMustSupplyError("server exec")
	}
	err := CheckExec(sc.ServerExec, "server exec")
	if err != nil {
		return err
	}
	return CheckExec(sc.InitExec, "initdb exec")
}

func (sc *postgresServerConfigImpl) SetDefaults() error {
	if sc.Host == "" {
		sc.Host = defaultHost
	}
	return nil
}
