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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrActiveServerLock = errors.NewKind("database locked by another sql-server; either clone the database to run a second server, or delete the '%s' if no other sql-servers are active")

// EnvNameAndPath is a simple tuple of the name of an environment and the path to where it is on disk
type EnvNameAndPath struct {
	// Name is the name of the environment and is used as the identifier when accessing a given environment
	Name string
	// Path is the path on disk to where the environment lives
	Path string
}

// MultiRepoEnv is a type used to store multiple environments which can be retrieved by name
type MultiRepoEnv struct {
	envs           []NamedEnv
	fs             filesys.Filesys
	cfg            config.ReadWriteConfig
	ignoreLockFile bool
}

type NamedEnv struct {
	name string
	env  *DoltEnv
}

func (mrEnv *MultiRepoEnv) FileSystem() filesys.Filesys {
	return mrEnv.fs
}

func (mrEnv *MultiRepoEnv) RemoteDialProvider() dbfactory.GRPCDialProvider {
	for _, env := range mrEnv.envs {
		return env.env
	}
	return NewGRPCDialProvider()
}

func (mrEnv *MultiRepoEnv) Config() config.ReadWriteConfig {
	return mrEnv.cfg
}

// TODO: un export
// AddEnv adds an environment to the MultiRepoEnv by name
func (mrEnv *MultiRepoEnv) AddEnv(name string, dEnv *DoltEnv) {
	mrEnv.envs = append(mrEnv.envs, NamedEnv{
		name: name,
		env:  dEnv,
	})
}

// AddOrReplaceEnvs adds the specified DoltEnv to this MultiRepoEnv, replacing
// any existing environment in the MultiRepoEnv with the same name.
func (mrEnv *MultiRepoEnv) AddOrReplaceEnv(name string, dEnv *DoltEnv) {
	// TODO: Modeling NamedEnvs as a map could probably simplify this file
	newNamedEnvs := make([]NamedEnv, 0, len(mrEnv.envs))
	for _, namedEnv := range mrEnv.envs {
		if namedEnv.name != name {
			newNamedEnvs = append(newNamedEnvs, namedEnv)
		}
	}
	newNamedEnvs = append(newNamedEnvs, NamedEnv{name: name, env: dEnv})

	mrEnv.envs = newNamedEnvs
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

// GetWorkingRoots returns a map with entries for each environment name with a value equal to the working root
// for that environment
func (mrEnv *MultiRepoEnv) GetWorkingRoots(ctx context.Context) (map[string]*doltdb.RootValue, error) {
	roots := make(map[string]*doltdb.RootValue)
	err := mrEnv.Iter(func(name string, dEnv *DoltEnv) (stop bool, err error) {
		root, err := dEnv.WorkingRoot(ctx)

		if err != nil {
			return true, err
		}

		roots[name] = root
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return roots, err
}

// IsLocked returns true if any env is locked
func (mrEnv *MultiRepoEnv) IsLocked() (bool, string) {
	if mrEnv.ignoreLockFile {
		return false, ""
	}

	for _, e := range mrEnv.envs {
		if e.env.IsLocked() {
			return true, e.env.LockFile()
		}
	}
	return false, ""
}

// Lock locks all child envs. If an error is returned, all
// child envs will be returned with their initial lock state.
func (mrEnv *MultiRepoEnv) Lock() error {
	if mrEnv.ignoreLockFile {
		return nil
	}

	if ok, f := mrEnv.IsLocked(); ok {
		return ErrActiveServerLock.New(f)
	}

	var err error
	for _, e := range mrEnv.envs {
		err = e.env.Lock()
		if err != nil {
			mrEnv.Unlock()
			return err
		}
	}
	return nil
}

// Unlock unlocks all child envs.
func (mrEnv *MultiRepoEnv) Unlock() error {
	if mrEnv.ignoreLockFile {
		return nil
	}

	var err, retErr error
	for _, e := range mrEnv.envs {
		err = e.env.Unlock()
		if err != nil && retErr == nil {
			retErr = err
		}
	}
	return retErr
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

// MultiEnvForDirectory returns a MultiRepoEnv for the directory rooted at the file system given
func MultiEnvForDirectory(
	ctx context.Context,
	config config.ReadWriteConfig,
	fs filesys.Filesys,
	version string,
	ignoreLockFile bool,
	oldDEnv *DoltEnv, // TODO: eventually get rid of this
) (*MultiRepoEnv, error) {
	mrEnv := &MultiRepoEnv{
		envs:           make([]NamedEnv, 0),
		fs:             fs,
		cfg:            config,
		ignoreLockFile: ignoreLockFile,
	}

	// Load current fs and put into mr env
	var dEnv *DoltEnv
	var dbName string
	// Only directly copy the oldDEnv for in-memory filesystems; something is wrong with loading them
	if _, ok := fs.(*filesys.InMemFS); ok {
		dbName = "dolt"
		dEnv = oldDEnv
	} else {
		path, err := fs.Abs("")
		if err != nil {
			return nil, err
		}
		envName := getRepoRootDir(path, string(os.PathSeparator))
		dbName = dirToDBName(envName)
		dEnv = oldDEnv
		// TODO: idk how or why, but this breaks docs.bats
		//dEnv = Load(ctx, GetCurrentUserHomeDir, fs, doltdb.LocalDirDoltDB, version)
	}

	envSet := map[string]*DoltEnv{}
	if dEnv.Valid() {
		envSet[dbName] = dEnv
	}

	// If there are other directories in the directory, try to load them as additional databases
	fs.Iter(".", false, func(path string, size int64, isDir bool) (stop bool) {
		if !isDir {
			return false
		}

		dir := filepath.Base(path)

		newFs, err := fs.WithWorkingDir(dir)
		if err != nil {
			return false
		}

		newEnv := Load(ctx, GetCurrentUserHomeDir, newFs, doltdb.LocalDirDoltDB, dEnv.Version)
		if newEnv.Valid() {
			envSet[dirToDBName(dir)] = newEnv
		}
		return false
	})

	enforceSingleFormat(envSet)

	// if the current directory database is in our set, add it first so it will be the current database
	var ok bool
	if dEnv, ok = envSet[dbName]; ok {
		mrEnv.AddEnv(dbName, dEnv)
		delete(envSet, dbName)
	}
	for dbName, dEnv = range envSet {
		mrEnv.AddEnv(dbName, dEnv)
	}

	return mrEnv, nil
}

// MultiEnvForPaths takes a variable list of EnvNameAndPath objects loads each of the environments, and returns a new
// MultiRepoEnv
func MultiEnvForPaths(
	ctx context.Context,
	hdp HomeDirProvider,
	cfg config.ReadWriteConfig,
	fs filesys.Filesys,
	version string,
	ignoreLockFile bool,
	envNamesAndPaths ...EnvNameAndPath,
) (*MultiRepoEnv, error) {
	nameToPath := make(map[string]string)
	for _, nameAndPath := range envNamesAndPaths {
		existingPath, ok := nameToPath[nameAndPath.Name]

		if ok {
			if existingPath == nameAndPath.Path {
				continue
			}

			return nil, fmt.Errorf("databases at paths '%s' and '%s' both attempted to load with the name '%s'", existingPath, nameAndPath.Path, nameAndPath.Name)
		}

		nameToPath[nameAndPath.Name] = nameAndPath.Path
	}

	mrEnv := &MultiRepoEnv{
		envs:           make([]NamedEnv, 0),
		fs:             fs,
		cfg:            cfg,
		ignoreLockFile: ignoreLockFile,
	}

	envSet := map[string]*DoltEnv{}
	for name, path := range nameToPath {
		absPath, err := fs.Abs(path)

		if err != nil {
			return nil, err
		}

		fsForEnv, err := filesys.LocalFilesysWithWorkingDir(absPath)

		if err != nil {
			return nil, err
		}

		urlStr := earl.FileUrlFromPath(filepath.Join(absPath, dbfactory.DoltDataDir), os.PathSeparator)
		dEnv := Load(ctx, hdp, fsForEnv, urlStr, version)

		if dEnv.RSLoadErr != nil {
			return nil, fmt.Errorf("error loading environment '%s' at path '%s': %s", name, absPath, dEnv.RSLoadErr.Error())
		} else if dEnv.DBLoadError != nil {
			return nil, fmt.Errorf("error loading environment '%s' at path '%s': %s", name, absPath, dEnv.DBLoadError.Error())
		} else if dEnv.CfgLoadErr != nil {
			return nil, fmt.Errorf("error loading environment '%s' at path '%s': %s", name, absPath, dEnv.CfgLoadErr.Error())
		}
		envSet[name] = dEnv
	}

	enforceSingleFormat(envSet)
	for dbName, dEnv := range envSet {
		mrEnv.AddEnv(dbName, dEnv)
	}

	return mrEnv, nil
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

func DBNamesAndPathsFromDir(fs filesys.Filesys, path string) ([]EnvNameAndPath, error) {
	var envNamesAndPaths []EnvNameAndPath
	err := fs.Iter(path, false, func(path string, size int64, isDir bool) (stop bool) {
		if isDir {
			dirName := filepath.Base(path)
			if dirName[0] == '.' {
				return false
			}

			name := dirToDBName(dirName)
			envNamesAndPaths = append(envNamesAndPaths, EnvNameAndPath{Name: name, Path: path})
		}

		return false
	})

	if err != nil {
		return nil, err
	}

	return envNamesAndPaths, nil
}

func dirToDBName(dirName string) string {
	dbName := strings.TrimSpace(dirName)
	dbName = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) || r == '-' {
			return '_'
		}
		return r
	}, dbName)

	newDBName := strings.ReplaceAll(dbName, "__", "_")

	for dbName != newDBName {
		dbName = newDBName
		newDBName = strings.ReplaceAll(dbName, "__", "_")
	}

	return dbName
}
