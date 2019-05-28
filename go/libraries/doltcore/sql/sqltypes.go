package sql

import "github.com/attic-labs/noms/go/types"

var DoltToSQLType = map[types.NomsKind]string{
	types.StringKind: VARCHAR,
	types.BoolKind:   BOOL,
	types.FloatKind:  FLOAT_TYPE,
	types.IntKind:    INT,
	types.UintKind:   INT + " " + UNSIGNED,
	types.UUIDKind:   UUID,
}