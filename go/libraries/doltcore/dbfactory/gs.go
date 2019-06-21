package dbfactory

import (
	"cloud.google.com/go/storage"
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
	"github.com/liquidata-inc/ld/dolt/go/store/go/datas"
	"github.com/liquidata-inc/ld/dolt/go/store/go/nbs"
	"net/url"
)

// GSFactory is a DBFactory implementation for creating GCS backed databases
type GSFactory struct {
}

// CreateDB creates an GCS backed database
func (fact GSFactory) CreateDB(ctx context.Context, urlObj *url.URL, params map[string]string) (datas.Database, error) {
	var db datas.Database
	err := pantoerr.PanicToError("failed to create database", func() error {
		gcs, err := storage.NewClient(ctx)

		if err != nil {
			return err
		}

		gcsStore := nbs.NewGCSStore(ctx, urlObj.Host, urlObj.Path, gcs, defaultMemTableSize)
		db = datas.NewDatabase(gcsStore)

		return nil
	})

	return db, err
}
