package env

import (
	"context"
	"fmt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"path/filepath"
	"strings"
	"unicode"
)

type EnvNameAndPath struct {
	Name string
	Path string
}

type MultiRepoEnv map[string]*DoltEnv

func (mrEnv MultiRepoEnv) AddEnv(name string, dEnv *DoltEnv) {
	mrEnv[name] = dEnv
}

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

func LoadMultiEnv(ctx context.Context, hdp HomeDirProvider, fs filesys.Filesys, version string, envNamesAndPaths ...EnvNameAndPath) (MultiRepoEnv, error){
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

		dEnv := Load(ctx, hdp, fs, "file://" + absPath, version)

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

func LoadMultiEnvFromDir(ctx context.Context, hdp HomeDirProvider, fs filesys.Filesys, path, version string) (MultiRepoEnv, error) {
	var envNamesAndPaths []EnvNameAndPath
	err := fs.Iter(path, false, func(path string, size int64, isDir bool) (stop bool) {
		if isDir {
			dirName := filepath.Base(path)
			name := dirToDBName(dirName)
			envNamesAndPaths = append(envNamesAndPaths, EnvNameAndPath{Name:name, Path:path})
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