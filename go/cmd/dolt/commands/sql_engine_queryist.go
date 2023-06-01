package commands

import (
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/shopspring/decimal"
	"time"
)

type SqlEngineQueryist struct {
	se *engine.SqlEngine
}

func (s SqlEngineQueryist) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error) {
	schema, iter, err := s.se.Query(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	newIter := NewSqlEngineRowIter(iter, schema)
	return newIter.newSchema, newIter, nil
}

var _ cli.Queryist = SqlEngineQueryist{}

func NewSqlEngineQueryist(se *engine.SqlEngine) cli.Queryist {
	return SqlEngineQueryist{se}
}

type SqlEngineRowIter struct {
	iter      sql.RowIter
	oldSchema sql.Schema
	newSchema sql.Schema
}

func (s SqlEngineRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	row, err := s.iter.Next(ctx)
	if err != nil {
		return nil, err
	}

	sqlRow := make(sql.Row, len(row))
	for i, val := range row {
		if val != nil {
			newValue, err := s.valToString(ctx, i, val)
			if err != nil {
				return nil, err
			}
			sqlRow[i] = newValue
		}
	}

	return sqlRow, nil
}

func (s SqlEngineRowIter) valToString(ctx *sql.Context, index int, val interface{}) (string, error) {
	column := s.oldSchema[index]
	ct := column.Type
	var err error
	var newValue string
	switch t := val.(type) {
	case string:
		newValue = t
	case types.OkResult:
		newValue = "OK"
	case int8:
		newValue = fmt.Sprintf("%d", t)
	case int16:
		newValue = fmt.Sprintf("%d", t)
	case int32:
		newValue = fmt.Sprintf("%d", t)
	case float32:
		newValue = fmt.Sprintf("%g", t)
	case float64:
		newValue = fmt.Sprintf("%g", t)
	case int64:
		newValue = fmt.Sprintf("%d", t)
	case decimal.Decimal:
		decimalType := ct.(types.DecimalType_)
		scale := decimalType.Scale()
		newValue = t.StringFixed(int32(scale))
	case uint64:
		switch ct.(type) {
		case types.BitType_:
			newValue = string(rune(t))
		default:
			newValue = fmt.Sprintf("%d", t)
		}
	case time.Time:
		sqlVal, err := ct.SQL(ctx, nil, t)
		if err != nil {
			return "", fmt.Errorf("unexpected time type %T", t)
		}
		newValue = sqlVal.ToString()
	case types.Timespan:
		newValue = t.String()
	case []uint8:
		newValue = string(t)
	case types.JSONDocument:
		newValue, err = t.ToString(nil)
		if err != nil {
			return "", fmt.Errorf("error converting JSONDocument type %T to string", t)
		}
	default:
		return "", fmt.Errorf("unexpected type %T", t)
	}
	return newValue, nil
}

func (s SqlEngineRowIter) Close(c *sql.Context) error {
	return s.iter.Close(c)
}

var _ sql.RowIter = (*SqlEngineRowIter)(nil)

func NewSqlEngineRowIter(iter sql.RowIter, schema sql.Schema) *SqlEngineRowIter {
	newSchema := make(sql.Schema, len(schema))
	for i, col := range schema {
		newSchema[i] = &sql.Column{
			Name:     col.Name,
			Type:     types.LongText,
			Nullable: true,
		}
	}
	return &SqlEngineRowIter{
		iter:      iter,
		oldSchema: schema,
		newSchema: newSchema,
	}
}
