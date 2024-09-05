package diff

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

type DatabaseSchemaDelta struct {
	FromName string
	ToName   string
}

func GetDatabaseSchemaDeltas(ctx context.Context, fromRoot, toRoot doltdb.RootValue) ([]DatabaseSchemaDelta, error) {
	return nil, nil
}
