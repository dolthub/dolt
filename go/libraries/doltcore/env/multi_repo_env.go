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
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/liquidata-inc/dolt/go/libraries/utils/earl"

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

// GetWorkingRoots returns a map with entries for each environment name with a value equal to the working root
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

func getRepoRootDir(path, pathSeparator string) string {
	// filepath.Clean does not work with cross platform paths.  So can't test a windows path on a mac
	tokens := strings.Split(path, pathSeparator)

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

// DoltEnvAsMultiEnv returns a MultiRepoEnv which wraps the DoltEnv and names it based on the directory DoltEnv refers to
func DoltEnvAsMultiEnv(dEnv *DoltEnv) MultiRepoEnv {
	dbName := "dolt"
	u, err := earl.Parse(dEnv.urlStr)

	if err == nil {
		if u.Scheme == dbfactory.FileScheme {
			path, err := url.PathUnescape(u.Path)

			if err == nil {
				path, err = dEnv.FS.Abs(path)

				if err == nil {
					dirName := getRepoRootDir(path, string(os.PathSeparator))

					if dirName != "" {
						dbName = dirToDBName(dirName)
					}
				}
			}
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

		mrEnv.AddEnv(name, dEnv)
	}

	return mrEnv, nil
}

// LoadMultiEnvFromDir looks at each subfolder of the given path as a Dolt repository and attempts to return a MultiRepoEnv
// with initialized environments for each of those subfolder data repositories. subfolders whose name starts with '.' are
// skipped.
func LoadMultiEnvFromDir(ctx context.Context, hdp HomeDirProvider, fs filesys.Filesys, path, version string) (MultiRepoEnv, error) {
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
