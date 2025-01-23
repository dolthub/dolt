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
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	doltDir   = dbfactory.DoltDir
	nomsDir   = dbfactory.DataDir
	oldGenDir = "oldgen"

	manifestFile = "manifest"
	migrationRef = "migration"
)

var (
	targetFormat = types.Format_DOLT
	migrationMsg = fmt.Sprintf("migrating database to Noms Binary Format %s", targetFormat.VersionString())
)

// Environment is a migration environment.
type Environment struct {
	Migration     *env.DoltEnv
	Existing      *env.DoltEnv
	DropConflicts bool
}

// NewEnvironment creates a migration Environment for |existing|.
func NewEnvironment(ctx context.Context, existing *env.DoltEnv) (Environment, error) {
	mfs, err := getMigrateFS(existing.FS)
	if err != nil {
		return Environment{}, err
	}

	if err = initMigrationDB(ctx, existing, existing.FS, mfs); err != nil {
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

	migration := env.NewDoltEnv(
		existing.Version,
		config,
		existing.RepoState,
		mdb,
		mfs,
	)

	return Environment{
		Migration: migration,
		Existing:  existing,
	}, nil
}

func initMigrationDB(ctx context.Context, existing *env.DoltEnv, src, dest filesys.Filesys) (err error) {
	base, err := src.Abs(".")
	if err != nil {
		return err
	}

	ierr := src.Iter(doltDir, true, func(path string, size int64, isDir bool) (stop bool) {
		path, err = filepath.Rel(base, path)
		if err != nil {
			stop = true
			return
		}

		if isDir {
			err = dest.MkDirs(path)
			stop = err != nil
			return
		}
		if strings.Contains(path, nomsDir) {
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

	absPath, err := dest.Abs(filepath.Join(doltDir, nomsDir))
	if err != nil {
		return err
	}
	if err = dest.MkDirs(absPath); err != nil {
		return err
	}

	u, err := earl.Parse("file://" + filepath.ToSlash(absPath))
	if err != nil {
		return err
	}

	params := map[string]any{dbfactory.ChunkJournalParam: struct{}{}}
	ddb, err := doltdb.LoadDoltDBWithParams(ctx, targetFormat, u.String(), dest, params)
	vrw := ddb.ValueReadWriter()
	ns := ddb.NodeStore()
	db := doltdb.HackDatasDatabaseFromDoltDB(ddb)

	// write init commit for migration
	name, email, err := env.GetNameAndEmail(existing.Config)
	if err != nil {
		return err
	}

	meta, err := datas.NewCommitMeta(name, email, migrationMsg)
	if err != nil {
		return err
	}

	rv, err := doltdb.EmptyRootValue(ctx, vrw, ns)
	if err != nil {
		return err
	}

	ds, err := db.GetDataset(ctx, ref.NewInternalRef(migrationRef).String())
	if err != nil {
		return err
	}

	_, err = db.Commit(ctx, ds, rv.NomsValue(), datas.CommitOptions{Meta: meta})
	return nil
}

// SwapChunkStores atomically swaps the ChunkStores of |menv.Migration| and |menv.Existing|.
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

	return swapManifests(ctx, src, dest)
}

func swapManifests(ctx context.Context, src, dest filesys.Filesys) (err error) {
	// backup the current manifest
	manifest := filepath.Join(doltDir, nomsDir, manifestFile)
	bak := filepath.Join(doltDir, nomsDir, manifestFile+".bak")
	if err = filesys.CopyFile(manifest, bak, dest, dest); err != nil {
		return err
	}

	// backup the current oldgen manifest, if one exists
	gcManifest := filepath.Join(doltDir, nomsDir, oldGenDir, manifestFile)
	oldGen, _ := dest.Exists(gcManifest)
	if oldGen {
		bak = filepath.Join(doltDir, nomsDir, oldGenDir, manifestFile+".bak")
		if err = filesys.CopyFile(gcManifest, bak, dest, dest); err != nil {
			return err
		}
	}

	// copy manifest to |dest| under temporary name
	tmp := filepath.Join(doltDir, nomsDir, "temp-manifest")
	if err = filesys.CopyFile(manifest, tmp, src, dest); err != nil {
		return err
	}

	// delete current oldgen manifest
	if oldGen {
		if err = dest.Delete(gcManifest, true); err != nil {
			return err
		}
	}

	// atomically swap the manifests
	return dest.MoveFile(tmp, manifest)
	// exit immediately!
}

func getMigrateFS(existing filesys.Filesys) (filesys.Filesys, error) {
	uniq := fmt.Sprintf("dolt_migration_%d", time.Now().UnixNano())
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
