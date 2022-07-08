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

package import_benchmarker

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/tblcmds"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

type doltCommandFunc func(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int

// BenchmarkDoltImport returns a function that runs benchmarks for importing
// a test dataset into Dolt
func BenchmarkDoltImport(importTest *ImportBenchmarkTest) func(b *testing.B) {
	return func(b *testing.B) {
		doltImport(b, importTest)
	}
}

func doltImport(b *testing.B, importTest *ImportBenchmarkTest) {
	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }()

	commandFunc, commandStr, args, dEnv := getBenchmarkingTools(importTest)

	runBenchmark(b, commandFunc, commandStr, args, dEnv)
}

func runBenchmark(b *testing.B, commandFunc doltCommandFunc, commandStr string, args []string, dEnv *env.DoltEnv) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		status := commandFunc(context.Background(), commandStr, args, dEnv)
		if status != 0 {
			log.Fatalf("running benchmark failed with exit code... %d \n", status)
		}
	}
}

func getBenchmarkingTools(importTest *ImportBenchmarkTest) (commandFunc doltCommandFunc, commandStr string, args []string, dEnv *env.DoltEnv) {
	switch importTest.fileFormat {
	case csvExt:
		dEnv = getImportEnv(filesys.LocalFS, getWorkingDir(), importTest.doltExecPath)
		args = []string{"-c", "-f", testTable, importTest.filePath}
		commandStr = "dolt table import"
		commandFunc = tblcmds.ImportCmd{}.Exec
	case sqlExt:
		dEnv = getImportEnv(filesys.LocalFS, getWorkingDir(), importTest.doltExecPath)
		args = []string{}
		commandStr = "dolt sql"
		commandFunc = commands.SqlCmd{}.Exec

		stdin := getStdinForSQLBenchmark(filesys.LocalFS, importTest.filePath)
		os.Stdin = stdin
	case jsonExt:
		pathToSchemaFile := filepath.Join(getWorkingDir(), fmt.Sprintf("testSchema%s", importTest.fileFormat))
		dEnv = getImportEnv(filesys.LocalFS, getWorkingDir(), importTest.doltExecPath)
		args = []string{"-c", "-f", "-s", pathToSchemaFile, testTable, importTest.filePath}
		commandStr = "dolt table import"
		commandFunc = tblcmds.ImportCmd{}.Exec
	default:
		log.Fatalf("cannot import file, unsupported file format %s \n", importTest.fileFormat)
	}

	return commandFunc, commandStr, args, dEnv
}

func getImportEnv(fs filesys.Filesys, workingDir, doltExec string) *env.DoltEnv {
	initializeDoltRepoAtWorkingDir(fs, workingDir, doltExec)

	err := os.Chdir(workingDir)
	if err != nil {
		panic(err.Error())
	}

	return env.Load(context.Background(), env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB, "test")
}

func getStdinForSQLBenchmark(fs filesys.Filesys, pathToImportFile string) *os.File {
	content, err := fs.ReadFile(pathToImportFile)
	if err != nil {
		log.Fatal(err)
	}

	tmpfile, err := os.CreateTemp("", "temp")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Remove(tmpfile.Name()) // clean up

	if _, err := tmpfile.Write(content); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	f, err := os.Open(tmpfile.Name())
	if err != nil {
		log.Fatal(err)
	}

	return f
}

func execCommand(ctx context.Context, name string, arg ...string) *exec.Cmd {
	e := exec.CommandContext(ctx, name, arg...)
	return e
}

// getAmountOfGarbageGenerated computes the amount of garbage created by an import operation.
func getAmountOfGarbageGenerated(doltExec string) float64 {
	// 1. Get the size of the current .dolt directory
	originalSize, err := dirSizeMB(getWorkingDir())
	if err != nil {
		panic(err.Error())
	}

	// 2. Execute Garbage Collection
	init := execCommand(context.Background(), doltExec, "gc")
	init.Dir = getWorkingDir()
	err = init.Run()
	if err != nil {
		panic(err.Error())
	}

	// 3. Get the new size of the current .dolt directory
	newSize, err := dirSizeMB(getWorkingDir())

	// 4. Return result
	return originalSize - newSize
}

// cc: https://stackoverflow.com/questions/32482673/how-to-get-directory-total-size
func dirSizeMB(path string) (float64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})

	sizeMB := float64(size) / 1024.0 / 1024.0

	return sizeMB, err
}

// RemoveTempDoltDataDir is used to remove the .dolt repository
func RemoveTempDoltDataDir(fs filesys.Filesys, dir string) {
	doltDir := filepath.Join(dir, dbfactory.DoltDir)
	exists, _ := fs.Exists(doltDir)
	if exists {
		err := fs.Delete(doltDir, true)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func getWorkingDir() string {
	wd, _ := os.Getwd()
	return wd
}

// initializeDoltRepoAtWorkingDir calls the `dolt init` command on the workingDir to create a new Dolt repository.
func initializeDoltRepoAtWorkingDir(fs filesys.Filesys, workingDir, doltExec string) {
	RemoveTempDoltDataDir(fs, workingDir)

	init := execCommand(context.Background(), doltExec, "init")
	init.Dir = workingDir
	err := init.Run()
	if err != nil {
		panic(err.Error()) // Fix
	}
}
