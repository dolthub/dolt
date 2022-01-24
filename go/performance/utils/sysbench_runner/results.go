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

package sysbench_runner

import (
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	stampFormat    = time.RFC3339
	SqlStatsPrefix = "SQL statistics:"
	read           = "read"
	write          = "write"
	other          = "other"
	totalQueries   = "total"
	totalEvents    = "total number of events"
	min            = "min"
	avg            = "avg"
	max            = "max"
	percentile     = "percentile"
	sum            = "sum"
	transactions   = "transactions"
	queriesPerSec  = "queries"
	ignoredErrors  = "ignored errors"
	reconnects     = "reconnects"
	totalTimeSecs  = "total time"
)

var (
	ResultFileTemplate = "%s_%s_%s_sysbench_performance%s"

	ErrUnableToParseOutput    = errors.New("unable to parse output")
	ErrUnsupportedHeaderField = errors.New("unsupported header field")
)

// Result stores the output from a sysbench test run
type Result struct {
	// Id is the uuid of this result row
	Id string `json:"id"`

	//  SuiteId is the test suite id this test result is associated with
	SuiteId string `json:"suite_id"`

	// TestId is the unique id for this test
	TestId string `json:"test_id"`

	// RuntimeOS is the runtime platform
	RuntimeOS string `json:"runtime_os"`

	// RuntimeGoArch is the runtime architecture
	RuntimeGoArch string `json:"runtime_goarch"`

	// ServerName is the name of the server used for the benchmark
	ServerName string `json:"server_name"`

	// ServerVersion is the server's version
	ServerVersion string `json:"server_version"`

	// ServerParams are the params used to run the server
	ServerParams string `json:"server_params"`

	// TestName is the name of the test
	TestName string `json:"test_name"`

	// TestParams are the params used to run the test
	TestParams string `json:"test_params"`

	// CreatedAt is the time the result was created UTC
	CreatedAt string `json:"created_at"`

	// SqlReadQueries is the number of read queries performed
	SqlReadQueries int64 `json:"sql_read_queries"`

	// SqlWriteQueries is the number of write queries performed
	SqlWriteQueries int64 `json:"sql_write_queries"`

	// SqlOtherQueries is the number of other queries performed
	SqlOtherQueries int64 `json:"sql_other_queries"`

	// SqlTotalQueries is the number of total queries performed
	SqlTotalQueries int64 `json:"sql_total_queries"`

	// SqlTotalQueriesPerSecond is the number of queries per second
	SqlTotalQueriesPerSecond float64 `json:"sql_total_queries_per_second"`

	// TransactionsTotal is the number of transactions performed
	TransactionsTotal int64 `json:"sql_transactions_total"`

	// TransactionsPerSecond is the number of transactions per second
	TransactionsPerSecond float64 `json:"sql_transactions_per_second"`

	// IgnoredErrorsTotal is the number of errors ignored
	IgnoredErrorsTotal int64 `json:"sql_ignored_errors_total"`

	// IgnoredErrorsPerSecond is the number of errors ignored per second
	IgnoredErrorsPerSecond float64 `json:"sql_ignored_errors_per_second"`

	// ReconnectsTotal is the number of reconnects performed
	ReconnectsTotal int64 `json:"sql_reconnects_total"`

	// ReconnectsPerSecond is the number of reconnects per second
	ReconnectsPerSecond float64 `json:"sql_reconnects_per_second"`

	// TotalTimeSeconds is the total time elapsed for this test
	TotalTimeSeconds float64 `json:"total_time_seconds"`

	// TotalNumberOfEvents is the total number of events
	TotalNumberOfEvents int64 `json:"total_number_of_events"`

	// LatencyMinMS is the minimum latency in milliseconds
	LatencyMinMS float64 `json:"latency_minimum_ms"`

	// LatencyAvgMS is the average latency in milliseconds
	LatencyAvgMS float64 `json:"latency_average_ms"`

	// LatencyMaxMS is the maximum latency in milliseconds
	LatencyMaxMS float64 `json:"latency_maximum_ms"`

	// LatencyPercentile is the latency of the 95th percentile
	LatencyPercentile float64 `json:"latency_percentile"`

	// LatencySumMS is the latency sum in milliseconds
	LatencySumMS float64 `json:"latency_sum_ms"`
}

// Results is a slice of Result
type Results []*Result

// Stamp timestamps the result using the provided stamp function
func (r *Result) Stamp(stampFunc func() string) {
	if r.CreatedAt != "" || stampFunc == nil {
		return
	}
	r.CreatedAt = stampFunc()
}

// FromConfigsNewResult returns a new result with some fields set based on the provided configs
func FromConfigsNewResult(config *Config, serverConfig *ServerConfig, t *Test, suiteId string, idFunc func() string) (*Result, error) {
	serverParams := serverConfig.GetServerArgs()

	var getId func() string
	if idFunc == nil {
		getId = func() string {
			return uuid.New().String()
		}
	} else {
		getId = idFunc
	}

	var name string
	if t.FromScript {
		base := filepath.Base(t.Name)
		ext := filepath.Ext(base)
		name = strings.TrimSuffix(base, ext)
	} else {
		name = t.Name
	}

	return &Result{
		Id:            getId(),
		SuiteId:       suiteId,
		TestId:        t.id,
		RuntimeOS:     config.RuntimeOS,
		RuntimeGoArch: config.RuntimeGoArch,
		ServerName:    string(serverConfig.Server),
		ServerVersion: serverConfig.Version,
		ServerParams:  strings.Join(serverParams, " "),
		TestName:      name,
		TestParams:    strings.Join(t.Params, " "),
	}, nil
}

// FromOutputResult accepts raw sysbench run output and returns the Result
func FromOutputResult(output []byte, config *Config, serverConfig *ServerConfig, test *Test, suiteId string, idFunc func() string) (*Result, error) {
	result, err := FromConfigsNewResult(config, serverConfig, test, suiteId, idFunc)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(output), "\n")
	var process bool
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, SqlStatsPrefix) {
			process = true
			continue
		}
		if process {
			err := UpdateResult(result, trimmed)
			if err != nil {
				return result, err
			}
		}
	}
	return result, nil
}

// UpdateResult extracts the key and value from the given line and updates the given Result
func UpdateResult(result *Result, line string) error {
	lineParts := strings.Split(line, ":")
	key := strings.TrimSpace(lineParts[0])
	if len(lineParts) > 1 {
		rawVal := strings.TrimSpace(lineParts[1])
		err := updateResult(result, key, rawVal)
		if err != nil {
			return err
		}
	}
	return nil
}

// updateResult updates a field in the given Result with the given value
func updateResult(result *Result, key, val string) error {
	var k string
	if strings.Contains(key, percentile) {
		k = percentile
	} else {
		k = key
	}
	switch k {
	case read:
		i, err := fromStringInt64(val)
		if err != nil {
			return err
		}
		result.SqlReadQueries = i
	case write:
		i, err := fromStringInt64(val)
		if err != nil {
			return err
		}
		result.SqlWriteQueries = i
	case other:
		i, err := fromStringInt64(val)
		if err != nil {
			return err
		}
		result.SqlOtherQueries = i
	case totalQueries:
		i, err := fromStringInt64(val)
		if err != nil {
			return err
		}
		result.SqlTotalQueries = i
	case transactions:
		total, perSecond, err := FromValWithParens(val)
		if err != nil {
			return err
		}
		t, err := fromStringInt64(total)
		if err != nil {
			return err
		}
		p, err := fromStringFloat64(fromPerSecondVal(perSecond))
		if err != nil {
			return err
		}
		result.TransactionsTotal = t
		result.TransactionsPerSecond = p
	case queriesPerSec:
		_, perSecond, err := FromValWithParens(val)
		if err != nil {
			return err
		}
		p, err := fromStringFloat64(fromPerSecondVal(perSecond))
		if err != nil {
			return err
		}
		result.SqlTotalQueriesPerSecond = p
	case ignoredErrors:
		total, perSecond, err := FromValWithParens(val)
		t, err := fromStringInt64(total)
		if err != nil {
			return err
		}
		p, err := fromStringFloat64(fromPerSecondVal(perSecond))
		if err != nil {
			return err
		}
		result.IgnoredErrorsTotal = t
		result.IgnoredErrorsPerSecond = p
	case reconnects:
		total, perSecond, err := FromValWithParens(val)
		t, err := fromStringInt64(total)
		if err != nil {
			return err
		}
		p, err := fromStringFloat64(fromPerSecondVal(perSecond))
		if err != nil {
			return err
		}
		result.ReconnectsTotal = t
		result.ReconnectsPerSecond = p
	case totalTimeSecs:
		t, err := fromStringFloat64(fromSecondsVal(val))
		if err != nil {
			return err
		}
		result.TotalTimeSeconds = t
	case totalEvents:
		i, err := fromStringInt64(val)
		if err != nil {
			return err
		}
		result.TotalNumberOfEvents = i
	case min:
		f, err := fromStringFloat64(val)
		if err != nil {
			return err
		}
		result.LatencyMinMS = f
	case avg:
		f, err := fromStringFloat64(val)
		if err != nil {
			return err
		}
		result.LatencyAvgMS = f
	case max:
		f, err := fromStringFloat64(val)
		if err != nil {
			return err
		}
		result.LatencyMaxMS = f
	case percentile:
		f, err := fromStringFloat64(val)
		if err != nil {
			return err
		}
		result.LatencyPercentile = f
	case sum:
		f, err := fromStringFloat64(val)
		if err != nil {
			return err
		}
		result.LatencySumMS = f
	default:
		return nil
	}
	return nil
}

// FromValWithParens takes a string containing parens and
// returns the value outside the parens first, and the value
// inside the parens second
func FromValWithParens(val string) (string, string, error) {
	if val == "" {
		return "", "", nil
	}
	parts := strings.Split(val, "(")
	if len(parts) <= 1 {
		return strings.TrimSpace(parts[0]), "", nil
	}
	if len(parts) > 2 {
		return "", "", ErrUnableToParseOutput
	}
	return strings.TrimSpace(parts[0]), strings.Trim(parts[1], ")"), nil
}

func fromPerSecondVal(val string) string {
	return strings.TrimSuffix(val, " per sec.")
}

func fromSecondsVal(val string) string {
	return strings.TrimRight(val, "s")
}

func fromStringInt64(val string) (int64, error) {
	return strconv.ParseInt(val, 10, 64)
}

func fromStringFloat64(val string) (float64, error) {
	return strconv.ParseFloat(val, 64)
}
