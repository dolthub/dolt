// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbfactory

import (
	"context"
	"net/url"
	"os"
	"path/filepath"

	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/datas"
	"github.com/liquidata-inc/dolt/go/store/nbs"
	"github.com/liquidata-inc/dolt/go/store/types"
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
