// Copyright 2019 Dolthub, Inc.
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
	"errors"
	"net/url"
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
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
func (fact FileFactory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (datas.Database, types.ValueReadWriter, error) {
	path, err := url.PathUnescape(urlObj.Path)

	if err != nil {
		return nil, nil, err
	}

	path = filepath.FromSlash(path)
	path = urlObj.Host + path

	err = validateDir(path)
	if err != nil {
		return nil, nil, err
	}
	q := nbs.NewUnlimitedMemQuotaProvider()
	newGenSt, err := nbs.NewLocalStore(ctx, nbf.VersionString(), path, defaultMemTableSize, q)

	if err != nil {
		return nil, nil, err
	}

	oldgenPath := filepath.Join(path, "oldgen")
	err = validateDir(oldgenPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, nil, err
		}

		err = os.Mkdir(oldgenPath, os.ModePerm)
		if err != nil && !errors.Is(err, os.ErrExist) {
			return nil, nil, err
		}
	}

	oldGenSt, err := nbs.NewLocalStore(ctx, newGenSt.Version(), oldgenPath, defaultMemTableSize, q)

	if err != nil {
		return nil, nil, err
	}

	st := nbs.NewGenerationalCS(oldGenSt, newGenSt)
	// metrics?

	vrw := types.NewValueStore(st)

	return datas.NewTypesDatabase(vrw), vrw, nil
}

func validateDir(path string) error {
	info, err := os.Stat(path)

	if err != nil {
		return err
	} else if !info.IsDir() {
		return filesys.ErrIsFile
	}

	return nil
}
