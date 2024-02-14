// Copyright 2019-2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package benchmark_runner

import (
	"fmt"

	"github.com/google/uuid"
)

type tpccTestParamsImpl struct {
	// NumThreads represents the number of threads running queries concurrently.
	NumThreads int

	// ScaleFactor represents the number of warehouse to test this at scale.
	ScaleFactor int

	// Tables represents the number of tables created per warehouse.
	Tables int

	// TrxLevel represents what transaction level to use
	TrxLevel string

	// ReportCSV determines whether to report output as a csv.
	ReportCSV bool

	// ReportInterval defines how often the tpcc benchmark outputs performance stats.
	ReportInterval int

	// Time represents how long
	Time int
}

var _ TestParams = &tpccTestParamsImpl{}

// NewDefaultTpccParams returns default TpccTestParams.
func NewDefaultTpccParams() *tpccTestParamsImpl {
	return &tpccTestParamsImpl{
		NumThreads:     2, // TODO: When ready, expose as command line argument.
		ScaleFactor:    1,
		Tables:         1,
		TrxLevel:       tpccTransactionLevelRr,
		ReportCSV:      true,
		ReportInterval: 1,
		Time:           30,
	}
}

func (t *tpccTestParamsImpl) GetNumThreads() int {
	return t.NumThreads
}

func (t *tpccTestParamsImpl) GetScaleFactor() int {
	return t.ScaleFactor
}

func (t *tpccTestParamsImpl) GetTables() int {
	return t.Tables
}

func (t *tpccTestParamsImpl) GetTrxLevel() string {
	return t.TrxLevel
}

func (t *tpccTestParamsImpl) GetReportCSV() bool {
	return t.ReportCSV
}

func (t *tpccTestParamsImpl) GetReportInterval() int {
	return t.ReportInterval
}

func (t *tpccTestParamsImpl) GetTime() int {
	return t.Time
}

func (t *tpccTestParamsImpl) ToSlice() []string {
	params := make([]string, 0)
	params = append(params, fmt.Sprintf("%s=%d", tpccThreadsFlag, t.NumThreads))
	params = append(params, fmt.Sprintf("%s=%d", tpccScaleFlag, t.ScaleFactor))
	params = append(params, fmt.Sprintf("%s=%d", tpccTablesFlag, t.Tables))
	params = append(params, fmt.Sprintf("%s=%s", tpccTransactionLevelFlag, t.TrxLevel))
	params = append(params, fmt.Sprintf("%s=%t", tpccReportCsv, t.ReportCSV))
	params = append(params, fmt.Sprintf("%s=%d", tpccReportIntervalFlag, t.ReportInterval))
	params = append(params, fmt.Sprintf("%s=%d", tpccTimeFlag, t.Time))
	return params
}

// tpccTestImpl encapsulates an End to End prepare, run, cleanup test case.
type tpccTestImpl struct {
	// Id represents a unique test id
	Id string

	// Name represents the name of the test case
	Name string

	// Params are associated parameters this test runs with
	Params TpccTestParams
}

var _ Test = &tpccTestImpl{}

// NewTpccTest instantiates and returns a TPCC test.
func NewTpccTest(name string, params TpccTestParams) *tpccTestImpl {
	return &tpccTestImpl{
		Id:     uuid.New().String(),
		Name:   name,
		Params: params,
	}
}

func (t *tpccTestImpl) doltArgs(serverConfig ServerConfig) []string {
	args := make([]string, 0)
	args = append(args, defaultTpccParams...)
	args = append(args, fmt.Sprintf("%s=%s", tpccMysqlHostFlag, serverConfig.GetHost()))
	port := serverConfig.GetPort()
	if port > 0 {
		args = append(args, fmt.Sprintf("%s=%d", tpccMysqlPortFlag, serverConfig.GetPort()))
	}
	args = append(args, fmt.Sprintf("%s=%d", tpccMysqlPortFlag, serverConfig.GetPort()))
	args = append(args, fmt.Sprintf("%s=%s", tpccMysqlUserFlag, defaultMysqlUser))
	args = append(args, fmt.Sprintf("%s=%d", tpccTimeFlag, t.Params.GetTime()))
	args = append(args, fmt.Sprintf("%s=%d", tpccThreadsFlag, t.Params.GetNumThreads()))
	args = append(args, fmt.Sprintf("%s=%d", tpccReportIntervalFlag, t.Params.GetReportInterval()))
	args = append(args, fmt.Sprintf("%s=%d", tpccTablesFlag, t.Params.GetTables()))
	args = append(args, fmt.Sprintf("%s=%d", tpccScaleFlag, t.Params.GetScaleFactor()))
	args = append(args, fmt.Sprintf("%s=%s", tpccTransactionLevelFlag, t.Params.GetTrxLevel()))
	return args
}

func (t *tpccTestImpl) mysqlArgs(serverConfig ServerConfig) []string {
	args := make([]string, 0)
	args = append(args, defaultTpccParams...)
	host := serverConfig.GetHost()
	port := serverConfig.GetPort()
	args = append(args, fmt.Sprintf("%s=%s", tpccMysqlHostFlag, host))
	if host == defaultHost {
		args = append(args, fmt.Sprintf("%s=%s", tpccMysqlUserFlag, tpccMysqlUsername))
		args = append(args, fmt.Sprintf("%s=%s", tpccMysqlPasswordFlag, tpccPassLocal))
	} else {
		args = append(args, fmt.Sprintf("%s=%s", tpccMysqlUserFlag, defaultMysqlUser))
	}
	if port > 0 {
		args = append(args, fmt.Sprintf("%s=%d", tpccMysqlPortFlag, serverConfig.GetPort()))
	}
	args = append(args, fmt.Sprintf("%s=%d", tpccTimeFlag, t.Params.GetTime()))
	args = append(args, fmt.Sprintf("%s=%d", tpccThreadsFlag, t.Params.GetNumThreads()))
	args = append(args, fmt.Sprintf("%s=%d", tpccReportIntervalFlag, t.Params.GetReportInterval()))
	args = append(args, fmt.Sprintf("%s=%d", tpccTablesFlag, t.Params.GetTables()))
	args = append(args, fmt.Sprintf("%s=%d", tpccScaleFlag, t.Params.GetScaleFactor()))
	args = append(args, fmt.Sprintf("%s=%s", tpccTransactionLevelFlag, t.Params.GetTrxLevel()))
	return args
}

func (t *tpccTestImpl) doltgresArgs(serverConfig ServerConfig) []string {
	args := make([]string, 0)
	args = append(args, defaultTpccParams...)
	args = append(args, fmt.Sprintf("%s=%d", tpccTimeFlag, t.Params.GetTime()))
	args = append(args, fmt.Sprintf("%s=%d", tpccThreadsFlag, t.Params.GetNumThreads()))
	args = append(args, fmt.Sprintf("%s=%d", tpccReportIntervalFlag, t.Params.GetReportInterval()))
	args = append(args, fmt.Sprintf("%s=%d", tpccTablesFlag, t.Params.GetTables()))
	args = append(args, fmt.Sprintf("%s=%d", tpccScaleFlag, t.Params.GetScaleFactor()))
	args = append(args, fmt.Sprintf("%s=%s", tpccTransactionLevelFlag, t.Params.GetTrxLevel()))
	return args
}

func (t *tpccTestImpl) postgresArgs(serverConfig ServerConfig) []string {
	args := make([]string, 0)
	args = append(args, defaultTpccParams...)
	args = append(args, fmt.Sprintf("%s=%d", tpccTimeFlag, t.Params.GetTime()))
	args = append(args, fmt.Sprintf("%s=%d", tpccThreadsFlag, t.Params.GetNumThreads()))
	args = append(args, fmt.Sprintf("%s=%d", tpccReportIntervalFlag, t.Params.GetReportInterval()))
	args = append(args, fmt.Sprintf("%s=%d", tpccTablesFlag, t.Params.GetTables()))
	args = append(args, fmt.Sprintf("%s=%d", tpccScaleFlag, t.Params.GetScaleFactor()))
	args = append(args, fmt.Sprintf("%s=%s", tpccTransactionLevelFlag, t.Params.GetTrxLevel()))
	return args
}

// getArgs returns a test's args for all TPCC steps
func (t *tpccTestImpl) getArgs(serverConfig ServerConfig) []string {
	st := serverConfig.GetServerType()
	switch st {
	case Dolt:
		return t.doltArgs(serverConfig)
	case Doltgres:
		return t.doltgresArgs(serverConfig)
	case Postgres:
		return t.postgresArgs(serverConfig)
	case MySql:
		return t.mysqlArgs(serverConfig)
	default:
		panic(fmt.Sprintf("unexpected server type: %s", st))
	}
}

func (t *tpccTestImpl) GetId() string {
	return t.Id
}

func (t *tpccTestImpl) GetName() string {
	return t.Name
}

func (t *tpccTestImpl) GetParamsToSlice() []string {
	return t.Params.ToSlice()
}

func (t *tpccTestImpl) GetPrepareArgs(serverConfg ServerConfig) []string {
	args := make([]string, 0)
	serverArgs := t.getArgs(serverConfg)
	args = append(args, serverArgs...)
	args = append(args, sysbenchPrepareCommand)
	return args
}

func (t *tpccTestImpl) GetRunArgs(serverConfg ServerConfig) []string {
	args := make([]string, 0)
	serverArgs := t.getArgs(serverConfg)
	args = append(args, serverArgs...)
	args = append(args, sysbenchRunCommand)
	return args
}

func (t *tpccTestImpl) GetCleanupArgs(serverConfg ServerConfig) []string {
	args := make([]string, 0)
	serverArgs := t.getArgs(serverConfg)
	args = append(args, serverArgs...)
	args = append(args, sysbenchCleanupCommand)
	return args
}
