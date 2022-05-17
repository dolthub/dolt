package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"
)

const clients = 16
const iters = 10

var sqlScript = []string{
	"call dolt_checkout('main');",
	"select * from dolt_log order by date desc limit 10;",
}

var (
	database = "SHAQ"
	user     = "root"
	pass     = ""
	host     = "127.0.0.1"
	port     = "3306"
)

// Runs |sqlScript| concurrently on multiple clients.
// Useful for repoducing concurrency bugs.
func main() {
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		user, pass, host, port, database)

	db, err := sql.Open("mysql", connStr)
	maybeExit(err)

	eg, ctx := errgroup.WithContext(context.Background())

	for i := 0; i < clients; i++ {
		eg.Go(func() (err error) {
			conn, err := db.Conn(ctx)
			if err != nil {
				return err
			}
			defer func() {
				cerr := conn.Close()
				if err != nil {
					err = cerr
				}
			}()
			for j := 0; j < iters; j++ {
				if err = query(ctx, conn); err != nil {
					return err
				}
			}
			return
		})
	}
	maybeExit(eg.Wait())
}

func query(ctx context.Context, conn *sql.Conn) error {
	for i := range sqlScript {
		_, err := conn.ExecContext(ctx, sqlScript[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func maybeExit(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
