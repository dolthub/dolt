package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

var queries = [12]string{
	"create table test (pk int, `value` int, primary key(pk))",
	"describe test",
	"select * from test",
	"insert into test (pk, `value`) values (0,0)",
	"select * from test",
	"call dolt_add('-A')",
	"call dolt_commit('-m', 'added table test')",
	"call dolt_checkout('-b', 'mybranch')",
	"insert into test values (1,1)",
	"call dolt_commit('-a', '-m', 'updated test')",
	"call dolt_checkout('main')",
	"call dolt_merge('mybranch')"}

type ResFunc func(rows *sql.Rows) error

type StmtTest struct {
	Query string
	Args  []interface{}
	Res   []ResFunc
}

var stmtTests []StmtTest = []StmtTest{
	{
		"select * from test where pk = ?",
		[]interface{}{int64(0)},
		[]ResFunc{
			func(rows *sql.Rows) error {
				var pk, value int64
				if err := rows.Scan(&pk, &value); err != nil {
					return err
				}
				if pk != 0 || value != 0 {
					return fmt.Errorf("Unexpected values for pk or value: %d, %d", pk, value)
				}
				return nil
			},
		},
	},
	{
		Query: "SELECT COUNT(*) FROM dolt_log",
		Res: []ResFunc{
			func(rows *sql.Rows) error {
				var size int64
				if err := rows.Scan(&size); err != nil {
					return err
				}
				if size != 3 {
					return fmt.Errorf("Unexpected values for size: %d", size)
				}
				return nil
			},
		},
	},
	{
		Query: "select count(*) from test",
		Res: []ResFunc{
			func(rows *sql.Rows) error {
				var size int64
				if err := rows.Scan(&size); err != nil {
					return err
				}
				if size != 2 {
					return fmt.Errorf("Unexpected values for size: %d", size)
				}
				return nil
			},
		},
	},
}

func main() {
	var user = os.Args[1]
	var port = os.Args[2]
	var db = os.Args[3]

	var dsn = user + "@tcp(127.0.0.1:" + port + ")/" + db
	fmt.Println(dsn)

	database, err := sql.Open("mysql", dsn)

	if err != nil {
		panic(err)
	}

	defer database.Close()

	// Ping opens a connection
	err = database.Ping()
	if err != nil {
		panic(err)
	}

	for _, query := range queries {
		rows, err := database.Query(query)
		if err != nil {
			fmt.Println("QUERY: " + query)
			panic(err)
		}

		// Ignoring result as only way to get results is rows.Next()
		// Requires custom typoing of the results.

		rows.Close()
	}

	for _, test := range stmtTests {
		func() {
			stmt, err := database.Prepare(test.Query)
			if err != nil {
				panic(fmt.Sprintf("err on prepare: %s: %v", test.Query, err))
			}
			defer func() {
				if err := stmt.Close(); err != nil {
					panic(fmt.Sprintf("err on stmt.Close(): %s: %v", test.Query, err))
				}
			}()
			rows, err := stmt.Query(test.Args...)
			if err != nil {
				panic(fmt.Sprintf("err on query: %s: %v", test.Query, err))
			}
			defer func() {
				if err := rows.Close(); err != nil {
					panic(fmt.Sprintf("err on rows.Close(): %s: %v", test.Query, err))
				}
			}()
			i := 0
			for rows.Next() {
				if i >= len(test.Res) {
					panic(fmt.Sprintf("statement returned more results than expected: %s: %v", test.Query, err))
				}
				if err := test.Res[i](rows); err != nil {
					panic(fmt.Sprintf("test.Res returned error: %d: %s: %v", i, test.Query, err))
				}
				i += 1
			}
			if err := rows.Err(); err != nil {
				panic(fmt.Sprintf("err on rows.Err(): %s: %v", test.Query, err))
			}
		}()
	}

	os.Exit(0)
}
