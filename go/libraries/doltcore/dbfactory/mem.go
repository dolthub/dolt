package dbfactory

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/datas"
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
