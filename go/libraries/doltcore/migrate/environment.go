// Copyright 2022 Dolthub, Inc.
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

package migrate

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

const (
	doltDir = dbfactory.DoltDir
	nomsDir = dbfactory.DataDir
)

var targetFormat = types.Format_DOLT_DEV

type Environment struct {
	Migration *env.DoltEnv
	Existing  *env.DoltEnv
}

func NewEnvironment(ctx context.Context, existing *env.DoltEnv) (Environment, error) {
	mfs, err := getMigrateFS(existing.FS)
	if err != nil {
		return Environment{}, err
	}

	if err = InitMigrationDB(ctx, existing.FS, mfs); err != nil {
		return Environment{}, err
	}

	mdb, err := doltdb.LoadDoltDB(ctx, targetFormat, doltdb.LocalDirDoltDB, mfs)
	if err != nil {
		return Environment{}, err
	}

	config, err := env.LoadDoltCliConfig(env.GetCurrentUserHomeDir, mfs)
	if err != nil {
		return Environment{}, err
	}

	migration := &env.DoltEnv{
		Version:   existing.Version,
		Config:    config,
		RepoState: existing.RepoState,
		DoltDB:    mdb,
		FS:        mfs,
		//urlStr:      urlStr,
		//hdp:         hdp,
	}

	return Environment{
		Migration: migration,
		Existing:  existing,
	}, nil
}

func SwapChunkstores(ctx context.Context, migration *env.DoltEnv, dest string) error {
	return nil
}

func InitMigrationDB(ctx context.Context, src, dest filesys.Filesys) (err error) {
	base, err := src.Abs(".")
	if err != nil {
		return err
	}

	ierr := src.Iter(doltDir, true, func(path string, size int64, isDir bool) (stop bool) {
		if isDir {
			err = dest.MkDirs(path)
			stop = err != nil
			return
		}
		if strings.Contains(path, nomsDir) {
			return
		}

		path, err = filepath.Rel(base, path)
		if err != nil {
			stop = true
			return
		}

		if err = filesys.CopyFile(path, path, src, dest); err != nil {
			stop = true
			return
		}
		return
	})
	if ierr != nil {
		return ierr
	}
	if err != nil {
		return err
	}

	dd, err := dest.Abs(filepath.Join(doltDir, nomsDir))
	if err != nil {
		return err
	}
	if err = dest.MkDirs(dd); err != nil {
		return err
	}

	u, err := earl.Parse(dd)
	if err != nil {
		return err
	}

	_, _, _, err = dbfactory.FileFactory{}.CreateDB(ctx, targetFormat, u, nil)
	return
}

func getMigrateFS(existing filesys.Filesys) (filesys.Filesys, error) {
	uniq := fmt.Sprintf("dolt_migration_%d", time.Now().Unix())
	tmpPath := filepath.Join(existing.TempDir(), uniq)
	if err := existing.MkDirs(tmpPath); err != nil {
		return nil, err
	}

	mfs, err := filesys.LocalFilesysWithWorkingDir(tmpPath)
	if err != nil {
		return nil, err
	}

	if err = mfs.MkDirs(doltDir); err != nil {
		return nil, err
	}
	return mfs, nil
}
