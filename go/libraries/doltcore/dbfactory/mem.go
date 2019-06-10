package dbfactory

import (
	"context"
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
	"net/url"
)

// MemFactory is a DBFactory implementation for creating in memory backed databases
type MemFactory struct {
}

// CreateDB creates an in memory backed database
func (fact MemFactory) CreateDB(ctx context.Context, urlObj *url.URL, params map[string]string) (datas.Database, error) {
	var db datas.Database
	err := pantoerr.PanicToError("failed to create database", func() error {
		storage := &chunks.MemoryStorage{}
		db = datas.NewDatabase(storage.NewView())

		return nil
	})

	return db, err
}
