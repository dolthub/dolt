package main

import (
	"fmt"
	"os"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
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

type StmtTest struct {
	Query string
	Args  []interface{}
	Res   [][]string
}

var stmtTests = []StmtTest{
	{
		Query: "select * from test where pk = ?",
		Args:  []interface{}{int64(0)},
		Res:   [][]string{
			{"0", "0"},
		},
	},
	{
		Query: "SELECT COUNT(*) FROM dolt_log",
		Res:   [][]string {
			{"3"},
		},
	},
	{
		Query: "select count(*) from test",
		Res:   [][]string{
			{"2"},
		},
	},
}

func main() {
	var user = os.Args[1]
	var port = os.Args[2]
	var db = os.Args[3]

	var addr = "127.0.0.1:" + port
	conn, err := client.Connect(addr, user, "", db)
	if err != nil {
		panic(err)
	}
	defer func() {
		err = conn.Close()
		if err != nil {
			panic(err)
		}
	}()

	// Ping opens a connection
	err = conn.Ping()
	if err != nil {
		panic(err)
	}

	// Setup database
	for _, query := range queries {
		// Ignoring result as only way to get results is rows.Next()
		// Requires custom typing of the results.
		_, err = conn.Execute(query)
		if err != nil {
			fmt.Println("QUERY: " + query)
			panic(err)
		}
	}

	for _, test := range stmtTests {
		func() {
			stmt, pErr := conn.Prepare(test.Query)
			if pErr != nil {
				panic(fmt.Sprintf("err on prepare: %s: %v", test.Query, pErr))
			}
			defer func() {
				if err = stmt.Close(); err != nil {
					panic(fmt.Sprintf("err on stmt.Close(): %s: %v", test.Query, err))
				}
			}()

			var result mysql.Result
			var rows [][]string
			err = stmt.ExecuteSelectStreaming(&result, func(row []mysql.FieldValue) error {
				resRow := make([]string, len(row))
				for i := 0; i < len(row); i++ {
					resRow[i] = row[i].String()
				}
				rows = append(rows, resRow)
				return nil
			}, nil, test.Args...)
			if err != nil {
				panic(fmt.Sprintf("err on query: %s: %v", test.Query, err))
			}
			if len(rows) != len(test.Res) {
				panic(fmt.Sprintf("expected %d rows, got %d: %s", len(test.Res), len(rows), test.Query))
			}

			for i := 0; i < len(test.Res); i++ {
				if test.Res[i][0] != rows[i][0] {
					panic(fmt.Sprintf("expected %s at row %d, got %s", test.Query, i, test.Query))
				}
			}
		}()
	}

	os.Exit(0)
}
