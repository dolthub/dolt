// Copyright 2019 Liquidata, Inc.
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

package logictest

// A Harness runs the queries in sqllogictest tests on an underlying SQL engine.
type Harness interface {
	// EngineStr returns the engine identifier string, used to skip tests that aren't supported on some engines. Valid
	// values include mysql, postgresql, and mssql.  See test files for other examples.
	EngineStr() string

	// Init initializes this harness to begin executing query records, beginning with a clean state for the underlying
	// database. Called once per test file before any tests are run. Harnesses are re-used between test files for runs
	// that request multiple test files, so this method should reset all relevant state.
	Init()

	// ExecuteStatement executes a DDL / insert / update statement on the underlying engine and returns any error. Some
	// tests expect errors. Any non-nil error satisfies a test that expects an error.
	ExecuteStatement(statement string) error

	// ExecuteQuery executes the query given and returns the results in the following format:
	// schema: a schema string for the schema of the result set, with one letter per column:
	//    I for integers
	//    R for floating points
	//    T for strings
	// results: a slice of results for the query, represented as strings, one column of each row per line, in the order
	// that the underlying engine returns them. Integer values are rendered as if by printf("%d"). Floating point values
	// are rendered as if by printf("%.3f"). NULL values are rendered as "NULL".
	// err: queries are never expected to return errors, so any error returned is counted as a failure.
	// For more information, see: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
	ExecuteQuery(statement string) (schema string, results []string, err error)
}
