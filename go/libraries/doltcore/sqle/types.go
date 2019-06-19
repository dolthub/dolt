package sqle

import (
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/src-d/go-mysql-server/sql"
)

func nomsTypeToSqlType(kind types.NomsKind) sql.Type {
	switch kind {
	case types.BoolKind:
		return sql.Boolean
	case types.FloatKind:
		return sql.Float64
	case types.StringKind:
		return sql.Text
	case types.UUIDKind:
		panic("TODO")
	case types.IntKind:
		return sql.Int64
	case types.UintKind:
		return sql.Uint64
	default:
		panic(fmt.Sprintf("Unexpected kind %v", kind))
	}
}
