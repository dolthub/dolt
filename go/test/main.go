package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"
)

func main() {
	var (
		dsn      string
		host     string
		port     int
		user     string
		password string
		database string
		timeout  time.Duration
	)

	flag.StringVar(&dsn, "dsn", "", "MySQL DSN (overrides host/port/user/password/database), e.g. root:@tcp(127.0.0.1:3306)/dolt")
	flag.StringVar(&host, "host", "127.0.0.1", "sql-server host")
	flag.IntVar(&port, "port", 3306, "sql-server port")
	flag.StringVar(&user, "user", "root", "sql-server user")
	flag.StringVar(&password, "password", "", "sql-server password")
	flag.StringVar(&database, "database", "dolt", "database name")
	flag.DurationVar(&timeout, "timeout", 5*time.Second, "overall connect/query timeout")
	flag.Parse()

	if dsn == "" {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", user, password, host, port, database)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sql.Open failed: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Ping failed: %v\n", err)
		os.Exit(1)
	}

	pushCtx, pushCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer pushCancel()

	eg, egCtx := errgroup.WithContext(pushCtx)
	for range 10 {
		eg.Go(func() error {
			_, err := db.ExecContext(egCtx, "call dolt_push('origin', 'main')")
			return err
		})
	}

	if err := eg.Wait(); err != nil {
		fmt.Fprintf(os.Stderr, "wait error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("successfully pushed to remote")
}
