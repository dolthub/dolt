package main

// A Harness runs the queries in sqllogictest tests on an underlying SQL engine.
type Harness interface {
	// EngineStr returns the engine identifier string, used to skip tests that aren't supported on some engines. Valid
	// values include mysql, postgresql, and mssql.  See test files for other examples.
	EngineStr() string

	// Init initializes this harness to begin executing query records. Called once per test file.
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
