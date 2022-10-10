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
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/pkg/errors"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"
)

// BenchmarkDoltImportJob returns a function that runs benchmarks for importing
// a test dataset into Dolt
func BenchmarkDoltImportJob(job *ImportBenchmarkJob, workingDir, nbf string) (result, error) {
	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }()

	err := setupAndInitializeDoltRepo(filesys.LocalFS, workingDir, job.ExecPath, nbf)
	if err != nil {
		return result{}, err
	}

	defer RemoveDoltDataDir(filesys.LocalFS, workingDir) // remove the repo each time

	commandStr, args, err := getBenchmarkingTools(job, workingDir)
	if err != nil {
		return result{}, err
	}

	if commandStr == "" {
		return result{}, errors.New("failed to get command")
	}

	br := testing.Benchmark(func(b *testing.B) {
		err = runBenchmarkCommand(b, commandStr, args, workingDir)
	})
	if err != nil {
		return result{}, err
	}

	size, err := getSizeOnDisk(filesys.LocalFS, workingDir)
	if err != nil {
		return result{}, err
	}

	return result{
		name:        job.Name,
		format:      job.Format,
		rows:        job.NumRows,
		columns:     len(genSampleCols()),
		sizeOnDisk:  size,
		br:          br,
		doltVersion: job.Version,
		program:     "dolt",
	}, nil
}

// setupAndInitializeDoltRepo calls the `dolt init` command on the workingDir to create a new Dolt repository.
func setupAndInitializeDoltRepo(fs filesys.Filesys, workingDir, doltExecPath, nbf string) error {
	err := RemoveDoltDataDir(fs, workingDir)
	if err != nil {
		return err
	}

	err = sysbench_runner.DoltVersion(context.Background(), doltExecPath)
	if err != nil {
		return err
	}

	err = sysbench_runner.UpdateDoltConfig(context.Background(), doltExecPath)
	if err != nil {
		return err
	}

	if nbf != "" {
		err = os.Setenv("DOLT_DEFAULT_BIN_FORMAT", nbf)
		if err != nil {
			return err
		}
	}

	init := execCommand(context.Background(), doltExecPath, "init")

	init.Dir = workingDir
	return init.Run()
}

// getBenchmarkingTools setups up the relevant environment for testing.
func getBenchmarkingTools(job *ImportBenchmarkJob, workingDir string) (commandStr string, args []string, err error) {
	commandStr = job.ExecPath

	switch job.Format {
	case csvExt:
		args = []string{"table", "import", "-c", "-f", testTable, job.Filepath}
		if job.SchemaPath != "" {
			args = append(args, "-s", job.SchemaPath)
		}
	case sqlExt:
		stdin, serr := getStdinForSQLBenchmark(filesys.LocalFS, job.Filepath)
		if serr != nil {
			err = serr
			return
		}

		os.Stdin = stdin

		args = []string{"sql"}
	case jsonExt:
		pathToSchemaFile := filepath.Join(workingDir, fmt.Sprintf("testSchema%s", job.Format))
		if job.SchemaPath != "" {
			pathToSchemaFile = job.SchemaPath
		}

		args = []string{"table", "import", "-c", "-f", "-s", pathToSchemaFile, testTable, job.Filepath}
	default:
		err = errors.New(fmt.Sprintf("cannot import file, unsupported file format %s \n", job.Format))
		return
	}

	return
}

// runBenchmarkCommand runs and times the benchmark. This is the critical portion of the code
func runBenchmarkCommand(b *testing.B, commandStr string, args []string, wd string) error {
	// Note that we can rerun this because dolt import uses the -f parameter
	for i := 0; i < b.N; i++ {
		cmd := execCommand(context.Background(), commandStr, args...)
		var errBytes bytes.Buffer
		cmd.Dir = wd
		cmd.Stdout = os.Stdout
		cmd.Stderr = &errBytes
		err := cmd.Run()
		if err != nil {
			return err
		}

		if len(strings.TrimSpace(errBytes.String())) > 0 {
			return errors.New(fmt.Sprintf("error running benchmark: %s", errBytes.String()))
		}
	}

	return nil
}

// RemoveDoltDataDir is used to remove the .dolt repository
func RemoveDoltDataDir(fs filesys.Filesys, dir string) error {
	doltDir := filepath.Join(dir, dbfactory.DoltDir)
	exists, _ := fs.Exists(doltDir)
	if exists {
		return fs.Delete(doltDir, true)
	}
	return nil
}

func execCommand(ctx context.Context, name string, arg ...string) *exec.Cmd {
	e := exec.CommandContext(ctx, name, arg...)
	return e
}

// getSizeOnDisk returns the size of the .dolt repo. This is useful for understanding how a repo grows in size in
// proportion to the number of rows.
func getSizeOnDisk(fs filesys.Filesys, workingDir string) (float64, error) {
	doltDir := filepath.Join(workingDir, dbfactory.DoltDir)
	exists, _ := fs.Exists(doltDir)

	if !exists {
		return 0, errors.New("dir does not exist")
	}

	size, err := dirSizeMB(doltDir)
	if err != nil {
		return 0, err
	}

	roundedStr := fmt.Sprintf("%.2f", size)
	rounded, _ := strconv.ParseFloat(roundedStr, 2)

	return rounded, nil
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

func getStdinForSQLBenchmark(fs filesys.Filesys, pathToImportFile string) (*os.File, error) {
	content, err := fs.ReadFile(pathToImportFile)
	if err != nil {
		return nil, err
	}

	tmpfile, err := os.CreateTemp("", "temp")
	if err != nil {
		return nil, err
	}
	defer file.Remove(tmpfile.Name()) // clean up

	if _, err := tmpfile.Write(content); err != nil {
		return nil, err
	}
	if err := tmpfile.Close(); err != nil {
		return nil, err
	}

	return os.Open(tmpfile.Name())
}
