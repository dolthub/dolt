// Copyright 2020 Liquidata, Inc.
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
	"path/filepath"
	"strings"
	"unicode"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

// EnvNameAndPath is a simple tuple of the name of an environment and the path to where it is on disk
type EnvNameAndPath struct {
	// Name is the name of the environment and is used as the identifier when accessing a given environment
	Name string
	// Path is the path on disk to where the environment lives
	Path string
}

// MultiRepoEnv is a type used to store multiple environments which can be retrieved by name
type MultiRepoEnv map[string]*DoltEnv

// AddEnv adds an environment to the MultiRepoEnv by name
func (mrEnv MultiRepoEnv) AddEnv(name string, dEnv *DoltEnv) {
	mrEnv[name] = dEnv
}

// Iter iterates over all environments in the MultiRepoEnv
func (mrEnv MultiRepoEnv) Iter(cb func(name string, dEnv *DoltEnv) (stop bool, err error)) error {
	for name, dEnv := range mrEnv {
		stop, err := cb(name, dEnv)

		if err != nil {
			return err
		}

		if stop {
			break
		}
	}

	return nil
}

// GetWorkingRoots gets returns a map with entries for each environment name with a value equal to the working root
// for that environment
func (mrEnv MultiRepoEnv) GetWorkingRoots(ctx context.Context) (map[string]*doltdb.RootValue, error) {
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

// DoltEnvAsMultiEnv returns a MultiRepoEnv which wraps the DoltEnv and names it based on the directory DoltEnv refers to
func DoltEnvAsMultiEnv(dEnv *DoltEnv) MultiRepoEnv {
	dbName := "dolt"
	filePrefix := dbfactory.FileScheme + "://"

	if strings.HasPrefix(dEnv.urlStr, filePrefix) {
		path, err := dEnv.FS.Abs(dEnv.urlStr[len(filePrefix):])

		if err == nil {
			if strings.HasSuffix(path, dbfactory.DoltDataDir) {
				path = path[:len(path)-len(dbfactory.DoltDataDir)]
			}

			if path[len(path)-1] == '/' {
				path = path[:len(path)-1]
			}

			idx := strings.LastIndex(path, "/")
			dirName := path[idx+1:]
			dbName = dirToDBName(dirName)
		}
	}

	mrEnv := make(MultiRepoEnv)
	mrEnv.AddEnv(dbName, dEnv)

	return mrEnv
}

// LoadMultiEnv takes a variable list of EnvNameAndPath objects loads each of the environments, and returns a new
// MultiRepoEnv
func LoadMultiEnv(ctx context.Context, hdp HomeDirProvider, fs filesys.Filesys, version string, envNamesAndPaths ...EnvNameAndPath) (MultiRepoEnv, error) {
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

	mrEnv := make(MultiRepoEnv)
	for name, path := range nameToPath {
		absPath, err := fs.Abs(path)

		if err != nil {
			return nil, err
		}

		dEnv := Load(ctx, hdp, fs, "file://"+absPath, version)

		if dEnv.RSLoadErr != nil {
			return nil, fmt.Errorf("error loading environment '%s' at path '%s': %s", name, absPath, dEnv.RSLoadErr.Error())
		} else if dEnv.DBLoadError != nil {
			return nil, fmt.Errorf("error loading environment '%s' at path '%s': %s", name, absPath, dEnv.DBLoadError.Error())
		} else if dEnv.CfgLoadErr != nil {
			return nil, fmt.Errorf("error loading environment '%s' at path '%s': %s", name, absPath, dEnv.CfgLoadErr.Error())
		}

		mrEnv.AddEnv(name, dEnv)
	}

	return mrEnv, nil
}

// LoadMultiEnvFromDir looks at each subfolder of the given path as a Dolt repository and attempts to return a MultiRepoEnv
// with initialized environments for each of those subfolder data repositories.
func LoadMultiEnvFromDir(ctx context.Context, hdp HomeDirProvider, fs filesys.Filesys, path, version string) (MultiRepoEnv, error) {
	var envNamesAndPaths []EnvNameAndPath
	err := fs.Iter(path, false, func(path string, size int64, isDir bool) (stop bool) {
		if isDir {
			dirName := filepath.Base(path)
			name := dirToDBName(dirName)
			envNamesAndPaths = append(envNamesAndPaths, EnvNameAndPath{Name: name, Path: path})
		}

		return false
	})

	if err != nil {
		return nil, err
	}

	return LoadMultiEnv(ctx, hdp, fs, version, envNamesAndPaths...)
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
