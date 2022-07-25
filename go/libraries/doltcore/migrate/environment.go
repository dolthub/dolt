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
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	doltDir      = dbfactory.DoltDir
	nomsDir      = dbfactory.DataDir
	manifestFile = "manifest"
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

	if err = InitMigrationDB(ctx, existing.DoltDB, existing.FS, mfs); err != nil {
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

func InitMigrationDB(ctx context.Context, existing *doltdb.DoltDB, src, dest filesys.Filesys) (err error) {
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

	db, vrw, ns, err := dbfactory.FileFactory{}.CreateDB(ctx, targetFormat, u, nil)
	if err != nil {
		return err
	}

	// migrate init commit
	init, err := getOrCreateInternalRef(ctx, existing)
	if err != nil {
		return err
	}

	meta, err := init.GetCommitMeta(ctx)
	if err != nil {
		return err
	}
	rv, err := doltdb.EmptyRootValue(ctx, vrw, ns)
	if err != nil {
		return err
	}
	nv := doltdb.HackNomsValuesFromRootValues(rv)

	creation := ref.NewInternalRef(doltdb.CreationBranch)
	ds, err := db.GetDataset(ctx, creation.String())
	if err != nil {
		return err
	}

	_, err = db.Commit(ctx, ds, nv, datas.CommitOptions{Meta: meta})
	return nil
}

func getOrCreateInternalRef(ctx context.Context, existing *doltdb.DoltDB) (*doltdb.Commit, error) {
	creation := ref.NewInternalRef(doltdb.CreationBranch)

	init, err := existing.ResolveCommitRef(ctx, creation)
	if err == nil && init != nil {
		return init, nil
	}
	if err != nil && !errors.Is(err, doltdb.ErrBranchNotFound) {
		return nil, err
	}

	heads, err := existing.GetBranches(ctx)
	if err != nil {
		return nil, err
	}
	cm, err := existing.ResolveCommitRef(ctx, heads[0])
	if err != nil {
		return nil, err
	}

	for {
		if cm.NumParents() < 1 {
			break
		}

		cm, err = cm.GetParent(ctx, 0)
		if err != nil {
			return nil, err
		}
	}

	h, err := cm.HashOf()
	if err != nil {
		return nil, err
	}

	err = existing.SetHead(ctx, creation, h)
	if err != nil {
		return nil, err
	}

	return cm, nil
}

func SwapChunkStores(ctx context.Context, menv Environment) error {
	src, dest := menv.Migration.FS, menv.Existing.FS

	absSrc, err := src.Abs(filepath.Join(doltDir, nomsDir))
	if err != nil {
		return err
	}

	absDest, err := dest.Abs(filepath.Join(doltDir, nomsDir))
	if err != nil {
		return err
	}

	var cpErr error
	err = src.Iter(absSrc, true, func(p string, size int64, isDir bool) (stop bool) {
		if strings.Contains(p, manifestFile) || isDir {
			return
		}

		var relPath string
		if relPath, cpErr = filepath.Rel(absSrc, p); cpErr != nil {
			stop = true
			return
		}

		srcPath := filepath.Join(absSrc, relPath)
		destPath := filepath.Join(absDest, relPath)

		if cpErr = filesys.CopyFile(srcPath, destPath, src, dest); cpErr != nil {
			stop = true
		}
		return
	})
	if err != nil {
		return err
	}
	if cpErr != nil {
		return cpErr
	}

	return SwapManifests(ctx, src, dest)
}

func SwapManifests(ctx context.Context, src, dest filesys.Filesys) (err error) {
	// backup the current manifest
	manifest := filepath.Join(doltDir, nomsDir, manifestFile)
	bak := filepath.Join(doltDir, nomsDir, manifestFile+".bak")
	if err = filesys.CopyFile(manifest, bak, dest, dest); err != nil {
		return err
	}

	// copy manifest to |dest| under temporary name
	tmp := filepath.Join(doltDir, nomsDir, "temp-manifest")
	if err = filesys.CopyFile(manifest, tmp, src, dest); err != nil {
		return err
	}

	// atomically swap the manifests
	return dest.MoveFile(tmp, manifest)
	// exit immediately!
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
