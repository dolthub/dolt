package main

import "os"
import "fmt"

import "database/sql"
import _ "github.com/go-sql-driver/mysql"

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

    os.Exit(1)
}
