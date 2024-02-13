package sysbench_runner

import (
	"fmt"
	"github.com/google/uuid"
)

type mysqlServerConfigImpl struct {
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

	// SkipLogBin will skip bin logging
	SkipLogBin bool

	// ServerArgs are the args used to start a server
	ServerArgs []string

	// ConnectionProtocol defines the protocol for connecting to the server
	ConnectionProtocol string

	// Socket is the path to the server socket
	Socket string
}

var _ ServerConfig = &mysqlServerConfigImpl{}

func NewMysqlServerConfig(version, serverExec, serverUser, host, resultsFormat, protocol, socket string, port int, serverArgs []string, skipBinLog bool) *mysqlServerConfigImpl {
	return &mysqlServerConfigImpl{
		Id:                 uuid.New().String(),
		Host:               host,
		Port:               port,
		Version:            version,
		ResultsFormat:      resultsFormat,
		ServerExec:         serverExec,
		ServerUser:         serverUser,
		SkipLogBin:         skipBinLog,
		ServerArgs:         serverArgs,
		ConnectionProtocol: protocol,
		Socket:             socket,
	}
}

func (sc *mysqlServerConfigImpl) GetServerExec() string {
	return sc.ServerExec
}

func (sc *mysqlServerConfigImpl) GetServerType() ServerType {
	return MySql
}

func (sc *mysqlServerConfigImpl) GetServerArgs() ([]string, error) {
	params := make([]string, 0)
	if sc.ServerUser != "" {
		params = append(params, fmt.Sprintf("%s=%s", userFlag, sc.ServerUser))
	}
	if sc.SkipLogBin {
		params = append(params, skipBinLogFlag)
	}
	if sc.Port != 0 {
		params = append(params, fmt.Sprintf("%s=%d", portFlag, sc.Port))
	}
	params = append(params, sc.ServerArgs...)
	return params, nil
}

func (sc *mysqlServerConfigImpl) GetTestingArgs(testConfig TestConfig) []string {
	params := make([]string, 0)
	params = append(params, defaultSysbenchParams...)
	params = append(params, fmt.Sprintf("--mysql-db=%s", dbName))
	params = append(params, "--db-driver=mysql")
	params = append(params, fmt.Sprintf("--mysql-host=%s", sc.Host))
	if sc.Port != 0 {
		params = append(params, fmt.Sprintf("--mysql-port=%d", sc.Port))
	}
	params = append(params, "--mysql-user=sysbench")
	params = append(params, fmt.Sprintf("--mysql-password=%s", sysbenchPassLocal))
	params = append(params, testConfig.GetOptions()...)
	params = append(params, testConfig.GetName())
	return params
}

func (sc *mysqlServerConfigImpl) Validate() error {
	if sc.Version == "" {
		return getMustSupplyError("version")
	}
	if sc.ResultsFormat == "" {
		return getMustSupplyError("results format")
	}
	if sc.ServerExec == "" {
		return getMustSupplyError("server exec")
	}
	err := CheckProtocol(sc.ConnectionProtocol)
	if err != nil {
		return err
	}
	return CheckExec(sc.ServerExec, "server exec")
}

func (sc *mysqlServerConfigImpl) SetDefaults() error {
	if sc.Host == "" {
		sc.Host = defaultHost
	}
	if sc.Port < 1 {
		sc.Port = defaultMysqlPort
	}
	return nil
}

func (sc *mysqlServerConfigImpl) GetId() string {
	return sc.Id
}

func (sc *mysqlServerConfigImpl) GetHost() string {
	return sc.Host
}

func (sc *mysqlServerConfigImpl) GetPort() int {
	return sc.Port
}
