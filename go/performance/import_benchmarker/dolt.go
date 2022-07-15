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

package import_benchmarker

import (
	"bytes"
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

// BenchmarkDoltImportJob returns a function that runs benchmarks for importing
// a test dataset into Dolt
func BenchmarkDoltImportJob(job *ImportBenchmarkJob, workingDir, nbf string) result {
	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }()

	setupAndInitializeDoltRepo(filesys.LocalFS, workingDir, job.ExecPath, nbf)
	defer RemoveDoltDataDir(filesys.LocalFS, workingDir) // remove the repo each time

	commandStr, args := getBenchmarkingTools(job, workingDir)

	br := testing.Benchmark(func(b *testing.B) {
		runBenchmarkCommand(b, commandStr, args, workingDir)
	})

	return result{
		name:        job.Name,
		format:      job.Format,
		rows:        job.NumRows,
		columns:     len(genSampleCols()),
		sizeOnDisk:  getSizeOnDisk(filesys.LocalFS, workingDir),
		br:          br,
		doltVersion: job.Version,
		program:     "dolt",
	}
}

// setupAndInitializeDoltRepo calls the `dolt init` command on the workingDir to create a new Dolt repository.
func setupAndInitializeDoltRepo(fs filesys.Filesys, workingDir, doltExecPath, nbf string) {
	RemoveDoltDataDir(fs, workingDir)

	err := sysbench_runner.DoltVersion(context.Background(), doltExecPath)
	if err != nil {
		log.Fatal(err.Error())
	}

	err = sysbench_runner.UpdateDoltConfig(context.Background(), doltExecPath)
	if err != nil {
		log.Fatal(err.Error())
	}

	if nbf != "" {
		err = os.Setenv("DOLT_DEFAULT_BIN_FORMAT", nbf)
		if err != nil {
			log.Fatal(err)
		}
	}

	init := execCommand(context.Background(), doltExecPath, "init")

	init.Dir = workingDir
	err = init.Run()
	if err != nil {
		log.Fatal(err.Error())
	}
}

// getBenchmarkingTools setups up the relevant environment for testing.
func getBenchmarkingTools(job *ImportBenchmarkJob, workingDir string) (commandStr string, args []string) {
	switch job.Format {
	case csvExt:
		args = []string{"table", "import", "-c", "-f", testTable, job.Filepath}
		if job.SchemaPath != "" {
			args = append(args, "-s", job.SchemaPath)
		}
	case sqlExt:
		stdin := getStdinForSQLBenchmark(filesys.LocalFS, job.Filepath)
		os.Stdin = stdin

		args = []string{"sql"}
	case jsonExt:
		pathToSchemaFile := filepath.Join(workingDir, fmt.Sprintf("testSchema%s", job.Format))
		if job.SchemaPath != "" {
			pathToSchemaFile = job.SchemaPath
		}

		args = []string{"table", "import", "-c", "-f", "-s", pathToSchemaFile, testTable, job.Filepath}
	default:
		log.Fatalf("cannot import file, unsupported file format %s \n", job.Format)
	}

	return job.ExecPath, args
}

// runBenchmarkCommand runs and times the benchmark. This is the critical portion of the code
func runBenchmarkCommand(b *testing.B, commandStr string, args []string, wd string) {
	// Note that we can rerun this because dolt import uses the -f parameter
	for i := 0; i < b.N; i++ {
		cmd := execCommand(context.Background(), commandStr, args...)
		var errBytes bytes.Buffer
		cmd.Dir = wd
		cmd.Stdout = os.Stdout
		cmd.Stderr = &errBytes
		err := cmd.Run()
		if err != nil {
			log.Fatalf("error running benchmark: %v", errBytes.String())
		}
	}
}

// RemoveDoltDataDir is used to remove the .dolt repository
func RemoveDoltDataDir(fs filesys.Filesys, dir string) {
	doltDir := filepath.Join(dir, dbfactory.DoltDir)
	exists, _ := fs.Exists(doltDir)
	if exists {
		err := fs.Delete(doltDir, true)
		if err != nil {
			log.Fatal(err)
		}
	}
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
