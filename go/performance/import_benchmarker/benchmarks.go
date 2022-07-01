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

package main

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

const (
	testHomeDir = "/user/tester"
)

type doltCommandFunc func(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int

func removeTempDoltDataDir(fs filesys.Filesys) {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	doltDir := filepath.Join(cwd, dbfactory.DoltDir)
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

// TODO: Think about the nbf type here
func initializeDoltRepoAtWorkingDir(fs filesys.Filesys, workingDir string) {
	removeTempDoltDataDir(fs)

	init := execCommand(context.Background(), "dolt", "init")
	init.Dir = workingDir
	err := init.Run()
	if err != nil {
		panic(err.Error()) // Fix
	}
}

// BenchmarkDoltImport returns a function that runs benchmarks for importing
// a test dataset into Dolt
func BenchmarkDoltImport(rows int, cols []*SeedColumn, format string) func(b *testing.B) {
	fs := filesys.LocalFS
	wd := getWorkingDir()
	return func(b *testing.B) {
		doltImport(b, fs, rows, cols, wd, format)
	}
}

// BenchmarkDoltExport returns a function that runs benchmarks for exporting
// a test dataset out of Dolt
func BenchmarkDoltExport(rows int, cols []*SeedColumn, format string) func(b *testing.B) {
	fs := filesys.LocalFS
	wd := getWorkingDir()
	return func(b *testing.B) {
		doltExport(b, fs, rows, cols, wd, format)
	}
}

// BenchmarkDoltSQLSelect returns a function that runs benchmarks for executing a sql query
// against a Dolt table
func BenchmarkDoltSQLSelect(rows int, cols []*SeedColumn, format string) func(b *testing.B) {
	fs := filesys.LocalFS
	wd := getWorkingDir()
	return func(b *testing.B) {
		doltSQLSelect(b, fs, rows, cols, wd, format)
	}
}

func doltImport(b *testing.B, fs filesys.Filesys, rows int, cols []*SeedColumn, workingDir, format string) {
	pathToImportFile := filepath.Join(workingDir, fmt.Sprintf("testData%s", format))

	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }()

	commandFunc, commandStr, args, dEnv := getBenchmarkingTools(fs, rows, cols, workingDir, pathToImportFile, format)

	runBenchmark(b, commandFunc, commandStr, args, dEnv)
}

func doltExport(b *testing.B, fs filesys.Filesys, rows int, cols []*SeedColumn, workingDir, format string) {
	pathToImportFile := filepath.Join(workingDir, fmt.Sprintf("testData%s", format))
	oldStdin := os.Stdin

	commandFunc, commandStr, args, dEnv := getBenchmarkingTools(fs, rows, cols, workingDir, pathToImportFile, format)

	// import
	status := commandFunc(context.Background(), commandStr, args, dEnv)
	if status != 0 {
		log.Fatalf("failed to import table successfully with exit code %d \n", status)
	}

	// revert stdin
	os.Stdin = oldStdin

	args = []string{"-f", "testTable", pathToImportFile}
	runBenchmark(b, tblcmds.ExportCmd{}.Exec, "dolt table export", args, dEnv)
}

func doltSQLSelect(b *testing.B, fs filesys.Filesys, rows int, cols []*SeedColumn, workingDir, format string) {
	testTable := "testTable"
	pathToImportFile := filepath.Join(workingDir, fmt.Sprintf("testData%s", format))

	oldStdin := os.Stdin

	commandFunc, commandStr, args, dEnv := getBenchmarkingTools(fs, rows, cols, workingDir, pathToImportFile, format)

	// import
	status := commandFunc(context.Background(), commandStr, args, dEnv)
	if status != 0 {
		log.Fatalf("failed to import table successfully with exit code %d \n", status)
	}

	// revert stdin
	os.Stdin = oldStdin

	args = []string{"-q", fmt.Sprintf("select count(*) from %s", testTable)}
	runBenchmark(b, commands.SqlCmd{}.Exec, "dolt sql", args, dEnv)
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

func getBenchmarkingTools(fs filesys.Filesys, rows int, cols []*SeedColumn, workingDir, pathToImportFile, format string) (commandFunc doltCommandFunc, commandStr string, args []string, dEnv *env.DoltEnv) {
	testTable := "testTable"
	sch := NewSeedSchema(rows, cols, format)

	switch format {
	case csvExt:
		dEnv = setupDEnvImport(fs, sch, workingDir, testTable, "", pathToImportFile)
		args = []string{"-c", "-f", testTable, pathToImportFile}
		commandStr = "dolt table import"
		commandFunc = tblcmds.ImportCmd{}.Exec
	case sqlExt:
		dEnv = setupDEnvImport(fs, sch, workingDir, testTable, "", pathToImportFile)
		args = []string{}
		commandStr = "dolt sql"
		commandFunc = commands.SqlCmd{}.Exec

		stdin := getStdinForSQLBenchmark(fs, pathToImportFile)
		os.Stdin = stdin
	case jsonExt:
		pathToSchemaFile := filepath.Join(workingDir, fmt.Sprintf("testSchema%s", format))
		dEnv = setupDEnvImport(fs, sch, workingDir, testTable, pathToSchemaFile, pathToImportFile)
		args = []string{"-c", "-f", "-s", pathToSchemaFile, testTable, pathToImportFile}
		commandStr = "dolt table import"
		commandFunc = tblcmds.ImportCmd{}.Exec
	default:
		log.Fatalf("cannot import file, unsupported file format %s \n", format)
	}

	return commandFunc, commandStr, args, dEnv
}

func setupDEnvImport(fs filesys.Filesys, sch *SeedSchema, workingDir, tableName, pathToSchemaFile, pathToImportFile string) *env.DoltEnv {
	wc, err := fs.OpenForWrite(pathToImportFile, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	defer wc.Close()

	ds := NewDSImpl(wc, sch, seedRandom, tableName)

	if pathToSchemaFile != "" {
		// write schema file
		err := fs.WriteFile(pathToSchemaFile, sch.Bytes())
		if err != nil {
			panic("unable to write data file to filesystem")
		}
	}

	ds.GenerateData()
	initializeDoltRepoAtWorkingDir(fs, workingDir)

	err = os.Chdir(workingDir)
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
