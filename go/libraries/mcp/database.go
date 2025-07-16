package mcp

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

type RowMap map[string]interface{}
type Columns []string

type Database interface {
	QueryContext(ctx context.Context, query string) (string, error)
	// ExecContext(ctx context.Context, query string) error
}

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
		db:     db,
		config: config,
	}, nil
}

func (d *databaseImpl) QueryContext(ctx context.Context, query string) (string, error) {
	rowMap, columns, err := d.doQueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	return d.rowMapToCSV(rowMap, columns)
}

func (d *databaseImpl) rowMapToCSV(rowMaps []RowMap, headers []string) (string, error) {
	var csvBuf strings.Builder
	writer := csv.NewWriter(&csvBuf)

	if err := writer.Write(headers); err != nil {
		return "", fmt.Errorf("failed to write headers: %v", err)
	}

	for _, rowMap := range rowMaps {
		row := make([]string, len(headers))
		for i, header := range headers {
			value, exists := rowMap[header]
			if !exists {
				return "", fmt.Errorf("key '%s' not found in map", header)
			}
			row[i] = fmt.Sprintf("%v", value)
		}
		if err := writer.Write(row); err != nil {
			return "", fmt.Errorf("failed to write row: %v", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", fmt.Errorf("error flushing CSV writer: %v", err)
	}

	return csvBuf.String(), nil
}

func (d *databaseImpl) doQueryContext(ctx context.Context, query string) ([]RowMap, Columns, error) {
	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	rowMaps := []RowMap{}
	for rows.Next() {
		// Create a slice of interface{}'s to hold each column value
		values := make([]interface{}, len(columns))

		// Create a slice of pointers to each value in values
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, nil, err
		}

		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			// Convert []byte to string for readability
			if b, ok := val.([]byte); ok {
				rowMap[col] = string(b)
			} else {
				rowMap[col] = val
			}
		}

		rowMaps = append(rowMaps, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	return rowMaps, columns, nil
}

func (d *databaseImpl) ExecContext(ctx context.Context, query string) (string, error) {
	return "", nil
}

func newDB(config Config) (*sql.DB, error) {
	dsn := config.GetDSN()

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
