package mcp

import (
	"fmt"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type Database interface{}

type databaseImpl struct {
	db     *sql.DB
	config Config
}

var _ Database = &databaseImpl{}

func NewDatabase(config Config) (Database, error) {
	db, err := newDB(config)
	if err != nil {
		return nil, err
	}
	return &databaseImpl{
		db: db,
		config: config,
	}, nil
}

func newDB(config Config) (*sql.DB, error) {
    dsn := config.GetDSN()
	fmt.Println("DUSTIN: dsn:", dsn)

    db, err := sql.Open("mysql", dsn)
    if err != nil {
        return nil, err
    }

	// todo: retry with backoff
    if err := db.Ping(); err != nil {
        return nil, err
    }

	return db, nil
}

