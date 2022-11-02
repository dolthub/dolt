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
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// FromResultCsvHeaders returns supported csv headers for a Result
func FromResultCsvHeaders() []string {
	return []string{
		"id",
		"suite_id",
		"test_id",
		"runtime_os",
		"runtime_goarch",
		"server_name",
		"server_version",
		"server_params",
		"test_name",
		"test_params",
		"created_at",

		// benchmark headers
		"sql_read_queries",
		"sql_write_queries",
		"sql_other_queries",
		"sql_total_queries",
		"sql_total_queries_per_second",
		"sql_transactions_total",
		"sql_transactions_per_second",
		"sql_ignored_errors_total",
		"sql_ignored_errors_per_second",
		"sql_reconnects_total",
		"sql_reconnects_per_second",
		"total_time_seconds",
		"total_number_of_events",
		"latency_minimum_ms",
		"latency_average_ms",
		"latency_maximum_ms",
		"latency_percentile",
		"latency_sum_ms",
	}
}

var floatTemplate = "%g"
var intTemplate = "%d"

// FromHeaderResultColumnValue returns the value from the Result for the given
// header field
func FromHeaderResultColumnValue(h string, r *Result) (string, error) {
	var val string
	switch h {
	case "id":
		val = r.Id
	case "suite_id":
		val = r.SuiteId
	case "test_id":
		val = r.TestId
	case "runtime_os":
		val = r.RuntimeOS
	case "runtime_goarch":
		val = r.RuntimeGoArch
	case "server_name":
		val = r.ServerName
	case "server_version":
		val = r.ServerVersion
	case "server_params":
		val = r.ServerParams
	case "test_name":
		val = r.TestName
	case "test_params":
		val = r.TestParams
	case "created_at":
		val = r.CreatedAt

	// benchmark headers
	case "sql_read_queries":
		val = fmt.Sprintf(intTemplate, r.SqlReadQueries)
	case "sql_write_queries":
		val = fmt.Sprintf(intTemplate, r.SqlWriteQueries)
	case "sql_other_queries":
		val = fmt.Sprintf(intTemplate, r.SqlOtherQueries)
	case "sql_total_queries":
		val = fmt.Sprintf(intTemplate, r.SqlTotalQueries)
	case "sql_total_queries_per_second":
		val = fmt.Sprintf(floatTemplate, r.SqlTotalQueriesPerSecond)
	case "sql_transactions_total":
		val = fmt.Sprintf(intTemplate, r.TransactionsTotal)
	case "sql_transactions_per_second":
		val = fmt.Sprintf(floatTemplate, r.TransactionsPerSecond)
	case "sql_ignored_errors_total":
		val = fmt.Sprintf(intTemplate, r.IgnoredErrorsTotal)
	case "sql_ignored_errors_per_second":
		val = fmt.Sprintf(floatTemplate, r.IgnoredErrorsPerSecond)
	case "sql_reconnects_total":
		val = fmt.Sprintf(intTemplate, r.ReconnectsTotal)
	case "sql_reconnects_per_second":
		val = fmt.Sprintf(floatTemplate, r.ReconnectsPerSecond)
	case "total_time_seconds":
		val = fmt.Sprintf(floatTemplate, r.TotalTimeSeconds)
	case "total_number_of_events":
		val = fmt.Sprintf(intTemplate, r.IgnoredErrorsTotal)
	case "latency_minimum_ms":
		val = fmt.Sprintf(floatTemplate, r.LatencyMinMS)
	case "latency_average_ms":
		val = fmt.Sprintf(floatTemplate, r.LatencyAvgMS)
	case "latency_maximum_ms":
		val = fmt.Sprintf(floatTemplate, r.LatencyMaxMS)
	case "latency_percentile":
		val = fmt.Sprintf(floatTemplate, r.LatencyPercentile)
	case "latency_sum_ms":
		val = fmt.Sprintf(floatTemplate, r.LatencySumMS)
	default:
		return "", ErrUnsupportedHeaderField
	}
	return val, nil
}

// FromHeaderResultFieldValue sets the value to the corresponding Result field for the given
// header field
func FromHeaderResultFieldValue(field string, val string, r *Result) error {
	switch field {
	case "id":
		r.Id = val
	case "suite_id":
		r.SuiteId = val
	case "test_id":
		r.TestId = val
	case "runtime_os":
		r.RuntimeOS = val
	case "runtime_goarch":
		r.RuntimeGoArch = val
	case "server_name":
		r.ServerName = val
	case "server_version":
		r.ServerVersion = val
	case "server_params":
		r.ServerParams = val
	case "test_name":
		r.TestName = val
	case "test_params":
		r.TestParams = val
	case "created_at":
		_, err := time.Parse(stampFormat, val)
		if err != nil {
			return err
		}
		r.CreatedAt = val

	// benchmark headers
	case "sql_read_queries":
		return updateResult(r, read, val)
	case "sql_write_queries":
		return updateResult(r, write, val)
	case "sql_other_queries":
		return updateResult(r, other, val)
	case "sql_total_queries":
		return updateResult(r, totalQueries, val)
	case "sql_total_queries_per_second":
		f, err := fromStringFloat64(val)
		if err != nil {
			return err
		}
		r.SqlTotalQueriesPerSecond = f
	case "sql_transactions_total":
		i, err := fromStringInt64(val)
		if err != nil {
			return err
		}
		r.TransactionsTotal = i
	case "sql_transactions_per_second":
		f, err := fromStringFloat64(val)
		if err != nil {
			return err
		}
		r.TransactionsPerSecond = f
	case "sql_ignored_errors_total":
		i, err := fromStringInt64(val)
		if err != nil {
			return err
		}
		r.IgnoredErrorsTotal = i
	case "sql_ignored_errors_per_second":
		f, err := fromStringFloat64(val)
		if err != nil {
			return err
		}
		r.IgnoredErrorsPerSecond = f
	case "sql_reconnects_total":
		i, err := fromStringInt64(val)
		if err != nil {
			return err
		}
		r.ReconnectsTotal = i
	case "sql_reconnects_per_second":
		f, err := fromStringFloat64(val)
		if err != nil {
			return err
		}
		r.ReconnectsPerSecond = f
	case "total_time_seconds":
		f, err := fromStringFloat64(val)
		if err != nil {
			return err
		}
		r.TotalTimeSeconds = f
	case "total_number_of_events":
		return updateResult(r, totalEvents, val)
	case "latency_minimum_ms":
		return updateResult(r, min, val)
	case "latency_average_ms":
		return updateResult(r, avg, val)
	case "latency_maximum_ms":
		return updateResult(r, max, val)
	case "latency_percentile":
		return updateResult(r, percentile, val)
	case "latency_sum_ms":
		return updateResult(r, sum, val)
	default:
		return ErrUnsupportedHeaderField
	}
	return nil
}

// WriteResultsCsv writes Results to a csv
func WriteResultsCsv(filename string, results Results) (err error) {
	dir := filepath.Dir(filename)
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return err
	}
	var file *os.File
	file, err = os.Create(filename)
	if err != nil {
		return
	}
	defer func() {
		closeErr := file.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	csvWriter := csv.NewWriter(file)

	// write header
	headers := FromResultCsvHeaders()
	if err := csvWriter.Write(headers); err != nil {
		return err
	}

	// write rows
	for _, r := range results {
		row := make([]string, 0)
		for _, field := range headers {
			val, err := FromHeaderResultColumnValue(field, r)
			if err != nil {
				return err
			}
			row = append(row, val)
		}
		err = csvWriter.Write(row)
		if err != nil {
			return err
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return err
	}
	return
}

// ReadResultsCsv reads a csv into Results
func ReadResultsCsv(filename string) (Results, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var textInput io.Reader = file

	reader := csv.NewReader(textInput)
	records, err := reader.ReadAll()
	if err != nil {
		return []*Result{}, err
	}

	var header []string
	results := make(Results, 0)
	for i, row := range records {
		// handle header
		if i == 0 {
			header = row
			continue
		}

		// handle rows
		r := &Result{}
		if header != nil {
			for j, field := range header {
				err := FromHeaderResultFieldValue(field, row[j], r)
				if err != nil {
					return []*Result{}, err
				}
			}
		}
		results = append(results, r)
	}

	return results, nil
}
