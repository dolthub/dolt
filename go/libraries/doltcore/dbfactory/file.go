package dbfactory

import (
	"context"
	"net/url"
	"os"
	"path/filepath"

	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/store/datas"
	"github.com/liquidata-inc/ld/dolt/go/store/nbs"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

const (
	// DoltDir defines the directory used to hold the dolt repo data within the filesys
	DoltDir = ".dolt"

	// DataDir is the directory internal to the DoltDir which holds the noms files.
	DataDir = "noms"
)

// DoltDataDir is the directory where noms files will be stored
var DoltDataDir = filepath.Join(DoltDir, DataDir)

// FileFactory is a DBFactory implementation for creating local filesys backed databases
type FileFactory struct {
}

// CreateDB creates an local filesys backed database
func (fact FileFactory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]string) (datas.Database, error) {
	path := urlObj.Host + urlObj.Path

	info, err := os.Stat(path)

	if err != nil {
		return nil, err
	} else if !info.IsDir() {
		return nil, filesys.ErrIsFile
	}

	st, err := nbs.NewLocalStore(ctx, nbf.VersionString(), path, defaultMemTableSize)

	if err != nil {
		return nil, err
	}

	return datas.NewDatabase(st), nil

}
