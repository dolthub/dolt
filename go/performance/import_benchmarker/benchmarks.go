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
	"strconv"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"
)

// SetupDoltImportBenchmark returns a function that runs benchmarks for importing
// a test dataset into Dolt
func SetupDoltImportBenchmark(importTest *ImportBenchmarkTest) func(b *testing.B) {
	return func(b *testing.B) {
		doltImport(b, importTest)
	}
}

func doltImport(b *testing.B, importTest *ImportBenchmarkTest) {
	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }()

	commandStr, args := getBenchmarkingTools(importTest)

	runBenchmark(b, commandStr, args, importTest)
}

func runBenchmark(b *testing.B, commandStr string, args []string, test *ImportBenchmarkTest) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := execCommand(context.Background(), commandStr, args...)
		cmd.Dir = test.workingDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if err != nil {
			log.Fatalf("error running benchmark: %v", err)
		}
	}
}

// getBenchmarkingTools setups up the relevant environment for testing.
func getBenchmarkingTools(importTest *ImportBenchmarkTest) (commandStr string, args []string) {
	initializeDoltRepoAtWorkingDir(filesys.LocalFS, importTest.workingDir, importTest.doltExecPath)

	switch importTest.fileFormat {
	case csvExt:
		args = []string{"table", "import", "-c", "-f", testTable, importTest.filePath}
	case sqlExt:
		stdin := getStdinForSQLBenchmark(filesys.LocalFS, importTest.filePath)
		os.Stdin = stdin

		args = []string{"sql"}
	case jsonExt:
		pathToSchemaFile := filepath.Join(importTest.workingDir, fmt.Sprintf("testSchema%s", importTest.fileFormat))
		args = []string{"table", "import", "-c", "-f", "-s", pathToSchemaFile, testTable, importTest.filePath}
	default:
		log.Fatalf("cannot import file, unsupported file format %s \n", importTest.fileFormat)
	}

	return importTest.doltExecPath, args
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

// initializeDoltRepoAtWorkingDir calls the `dolt init` command on the workingDir to create a new Dolt repository.
func initializeDoltRepoAtWorkingDir(fs filesys.Filesys, workingDir, doltExecPath string) {
	RemoveTempDoltDataDir(fs, workingDir)

	err := sysbench_runner.DoltVersion(context.Background(), doltExecPath)
	if err != nil {
		log.Fatal(err.Error())
	}

	err = sysbench_runner.UpdateDoltConfig(context.Background(), doltExecPath)
	if err != nil {
		log.Fatal(err.Error())
	}

	init := execCommand(context.Background(), doltExecPath, "init")

	init.Dir = workingDir
	err = init.Run()
	if err != nil {
		panic(err.Error())
	}
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

func GetWorkingDir() string {
	wd, _ := os.Getwd()
	return wd
}

func execCommand(ctx context.Context, name string, arg ...string) *exec.Cmd {
	e := exec.CommandContext(ctx, name, arg...)
	return e
}

// getSizeOnDisk returns the size of the .dolt repo. This is useful for understanding how a repo grows in size in
// proportion to the number of rows.
func getSizeOnDisk(fs filesys.Filesys, workingDir string) float64 {
	doltDir := filepath.Join(workingDir, dbfactory.DoltDir)
	exists, _ := fs.Exists(doltDir)

	if !exists {
		return 0
	}

	size, err := dirSizeMB(doltDir)
	if err != nil {
		log.Fatal(err.Error())
	}

	roundedStr := fmt.Sprintf("%.2f", size)
	rounded, _ := strconv.ParseFloat(roundedStr, 2)

	return rounded
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
