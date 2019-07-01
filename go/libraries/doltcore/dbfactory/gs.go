package dbfactory

import (
	"cloud.google.com/go/storage"
	"context"
	"github.com/liquidata-inc/ld/dolt/go/store/datas"
	"github.com/liquidata-inc/ld/dolt/go/store/nbs"
	"net/url"
)

// GSFactory is a DBFactory implementation for creating GCS backed databases
type GSFactory struct {
}

// CreateDB creates an GCS backed database
func (fact GSFactory) CreateDB(ctx context.Context, urlObj *url.URL, params map[string]string) (datas.Database, error) {
	var db datas.Database
	gcs, err := storage.NewClient(ctx)

	if err != nil {
		return nil, err
	}

	gcsStore, err := nbs.NewGCSStore(ctx, urlObj.Host, urlObj.Path, gcs, defaultMemTableSize)

	if err != nil {
		return nil, err
	}

	db = datas.NewDatabase(gcsStore)

	return db, err
}
