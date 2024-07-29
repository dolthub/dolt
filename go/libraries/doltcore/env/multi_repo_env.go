// Copyright 2020 Dolthub, Inc.
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

package env

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

// EnvNameAndPath is a simple tuple of the name of an environment and the path to where it is on disk
type EnvNameAndPath struct {
	// Name is the name of the environment and is used as the identifier when accessing a given environment
	Name string
	// Path is the path on disk to where the environment lives
	Path string
}

type NamedEnv struct {
	name string
	env  *DoltEnv
}

// MultiRepoEnv is a type used to store multiple environments which can be retrieved by name
type MultiRepoEnv struct {
	envs         []NamedEnv
	fs           filesys.Filesys
	cfg          config.ReadWriteConfig
	dialProvider dbfactory.GRPCDialProvider
}

type StorageMetadataMap map[string]nbs.StorageMetadata

func (sms StorageMetadataMap) ArchiveFilesPresent() bool {
	for _, sm := range sms {
		if sm.ArchiveFilesPresent() {
			return true
		}
	}
	return false
}

func GetMultiEnvStorageMetadata(dataDirFS filesys.Filesys) (StorageMetadataMap, error) {

	dbMap := make(map[string]filesys.Filesys)

	path, err := dataDirFS.Abs("")
	if err != nil {
		return nil, err
	}
	envName := getRepoRootDir(path, string(os.PathSeparator))
	dbName := dbfactory.DirToDBName(envName)
	dbMap[dbName] = dataDirFS

	// If there are other directories in the directory, try to load them as additional databases
	dataDirFS.Iter(".", false, func(path string, _ int64, isDir bool) (stop bool) {
		if !isDir {
			return false
		}

		dir := filepath.Base(path)

		newFs, er2 := dataDirFS.WithWorkingDir(dir)
		if er2 != nil {
			return false
		}
		path, er2 = newFs.Abs("")
		if er2 != nil {
			return false
		}
		envName := getRepoRootDir(path, string(os.PathSeparator))

		falseEnv := IncompleteEnv(newFs)
		if !falseEnv.Valid() {
			return false
		}

		dbName := dbfactory.DirToDBName(envName)
		dbMap[dbName] = dataDirFS

		return false
	})

	sms := make(StorageMetadataMap)
	for _, fs := range dbMap {
		fsStr, err := fs.Abs("")
		if err != nil {
			return nil, err
		}

		sm, err := nbs.GetStorageMetadata(fsStr)
		if err != nil {
			return nil, err
		}
		sms[fsStr] = sm
	}
	return sms, nil
}

// NewMultiEnv returns a new MultiRepoEnv instance dirived from a root DoltEnv instance.
func MultiEnvForSingleEnv(ctx context.Context, env *DoltEnv) (*MultiRepoEnv, error) {
	return MultiEnvForDirectory(ctx, env.Config.WriteableConfig(), env.FS, env.Version, env)
}

// MultiEnvForDirectory returns a MultiRepoEnv for the directory rooted at the file system given. The doltEnv from the
// invoking context is included. If it's non-nil and valid, it will be included in the returned MultiRepoEnv, and will
// be the first database in all iterations.
func MultiEnvForDirectory(
	ctx context.Context,
	config config.ReadWriteConfig,
	dataDirFS filesys.Filesys,
	version string,
	dEnv *DoltEnv,
) (*MultiRepoEnv, error) {
	// Load current dataDirFS and put into mr env
	var dbName string = "dolt"
	var newDEnv *DoltEnv = dEnv

	// InMemFS is used only for testing.
	// All other FS Types should get a newly created Environment which will serve as the primary env in the MultiRepoEnv
	if _, ok := dataDirFS.(*filesys.InMemFS); !ok {
		path, err := dataDirFS.Abs("")
		if err != nil {
			return nil, err
		}
		envName := getRepoRootDir(path, string(os.PathSeparator))
		dbName = dbfactory.DirToDBName(envName)

		newDEnv = Load(ctx, GetCurrentUserHomeDir, dataDirFS, doltdb.LocalDirDoltDB, version)
	}

	mrEnv := &MultiRepoEnv{
		envs:         make([]NamedEnv, 0),
		fs:           dataDirFS,
		cfg:          config,
		dialProvider: NewGRPCDialProviderFromDoltEnv(newDEnv),
	}

	envSet := map[string]*DoltEnv{}
	if newDEnv.Valid() {
		envSet[dbName] = newDEnv
	}

	// If there are other directories in the directory, try to load them as additional databases
	dataDirFS.Iter(".", false, func(path string, size int64, isDir bool) (stop bool) {
		if !isDir {
			return false
		}

		dir := filepath.Base(path)

		newFs, err := dataDirFS.WithWorkingDir(dir)
		if err != nil {
			return false
		}

		// TODO: get rid of version altogether
		version := ""
		if dEnv != nil {
			version = dEnv.Version
		}

		newEnv := Load(ctx, GetCurrentUserHomeDir, newFs, doltdb.LocalDirDoltDB, version)
		if newEnv.Valid() {
			envSet[dbfactory.DirToDBName(dir)] = newEnv
		} else {
			dbErr := newEnv.DBLoadError
			if dbErr != nil {
				if !errors.Is(dbErr, doltdb.ErrMissingDoltDataDir) {
					logrus.Warnf("failed to load database at %s with error: %s", path, dbErr.Error())
				}
			}
			cfgErr := newEnv.CfgLoadErr
			if cfgErr != nil {
				logrus.Warnf("failed to load database configuration at %s with error: %s", path, cfgErr.Error())
			}
		}
		return false
	})

	enforceSingleFormat(envSet)

	// if the current directory database is in our set, add it first so it will be the current database
	if env, ok := envSet[dbName]; ok && env.Valid() {
		mrEnv.addEnv(dbName, env)
		delete(envSet, dbName)
	}

	// get the keys from the envSet keys as a sorted list
	sortedKeys := make([]string, 0, len(envSet))
	for k := range envSet {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	for _, dbName := range sortedKeys {
		mrEnv.addEnv(dbName, envSet[dbName])
	}

	return mrEnv, nil
}

func (mrEnv *MultiRepoEnv) FileSystem() filesys.Filesys {
	return mrEnv.fs
}

func (mrEnv *MultiRepoEnv) RemoteDialProvider() dbfactory.GRPCDialProvider {
	return mrEnv.dialProvider
}

func (mrEnv *MultiRepoEnv) Config() config.ReadWriteConfig {
	return mrEnv.cfg
}

// addEnv adds an environment to the MultiRepoEnv by name
func (mrEnv *MultiRepoEnv) addEnv(name string, dEnv *DoltEnv) {
	mrEnv.envs = append(mrEnv.envs, NamedEnv{
		name: name,
		env:  dEnv,
	})
}

// GetEnv returns the env with the name given, or nil if no such env exists
func (mrEnv *MultiRepoEnv) GetEnv(name string) *DoltEnv {
	var found *DoltEnv
	mrEnv.Iter(func(n string, dEnv *DoltEnv) (stop bool, err error) {
		if n == name {
			found = dEnv
			return true, nil
		}
		return false, nil
	})
	return found
}

// Iter iterates over all environments in the MultiRepoEnv
func (mrEnv *MultiRepoEnv) Iter(cb func(name string, dEnv *DoltEnv) (stop bool, err error)) error {
	for _, e := range mrEnv.envs {
		stop, err := cb(e.name, e.env)

		if err != nil {
			return err
		}

		if stop {
			break
		}
	}

	return nil
}

// GetFirstDatabase returns the name of the first database in the MultiRepoEnv. This will be the database in the
// current working directory if applicable, or the first database alphabetically otherwise.
func (mrEnv *MultiRepoEnv) GetFirstDatabase() string {
	var currentDb string
	_ = mrEnv.Iter(func(name string, _ *DoltEnv) (stop bool, err error) {
		currentDb = name
		return true, nil
	})

	return currentDb
}

func getRepoRootDir(path, pathSeparator string) string {
	if pathSeparator != "/" {
		path = strings.ReplaceAll(path, pathSeparator, "/")
	}

	// filepath.Clean does not work with cross platform paths.  So can't test a windows path on a mac
	tokens := strings.Split(path, "/")

	for i := len(tokens) - 1; i >= 0; i-- {
		if tokens[i] == "" {
			tokens = append(tokens[:i], tokens[i+1:]...)
		}
	}

	if len(tokens) == 0 {
		return ""
	}

	if tokens[len(tokens)-1] == dbfactory.DataDir && tokens[len(tokens)-2] == dbfactory.DoltDir {
		tokens = tokens[:len(tokens)-2]
	}

	if len(tokens) == 0 {
		return ""
	}

	name := tokens[len(tokens)-1]

	// handles drive letters. fine with a folder containing a colon having the default name
	if strings.IndexRune(name, ':') != -1 {
		return ""
	}

	return name
}

// enforceSingleFormat enforces that constraint that all databases in
// a multi-database environment have the same NomsBinFormat.
// Databases are removed from the MultiRepoEnv to ensure this is true.
func enforceSingleFormat(envSet map[string]*DoltEnv) {
	formats := set.NewEmptyStrSet()
	for _, dEnv := range envSet {
		formats.Add(dEnv.DoltDB.Format().VersionString())
	}

	var nbf string
	// if present, prefer types.Format_Default
	if ok := formats.Contains(types.Format_Default.VersionString()); ok {
		nbf = types.Format_Default.VersionString()
	} else {
		// otherwise, pick an arbitrary format
		for _, dEnv := range envSet {
			nbf = dEnv.DoltDB.Format().VersionString()
		}
	}

	template := "incompatible format for database '%s'; expected '%s', found '%s'"
	for name, dEnv := range envSet {
		found := dEnv.DoltDB.Format().VersionString()
		if found != nbf {
			logrus.Infof(template, name, nbf, found)
			delete(envSet, name)
		}
	}
}
