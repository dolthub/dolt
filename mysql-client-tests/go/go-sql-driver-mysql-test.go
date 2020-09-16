package main

import "os"
import "fmt"

import "database/sql"
import _ "github.com/go-sql-driver/mysql"

var queries [5]string = [5]string{
    "create table test (pk int, value int, primary key(pk))",
    "describe test",
    "select * from test",
    "insert into test (pk, value) values (0,0)",
    "select * from test"}

func main() {
    var user = os.Args[1]
    var port = os.Args[2]
    var db   = os.Args[3]

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

    os.Exit(0)
}