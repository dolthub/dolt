// Copyright 2023 Dolthub, Inc.
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

package commands

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

var batseeDoc = cli.CommandDocumentationContent{
	ShortDesc: `Run the Bats Tests concurrently`,
	LongDesc:  `TODO`,
}

type BatseeCmd struct {
	messages []string
}

var _ cli.Command = &BatseeCmd{}
var _ cli.RepoNotRequiredCommand = &BatseeCmd{}

func (b BatseeCmd) Name() string {
	return "batsee"
}

func (b BatseeCmd) Description() string {
	return batseeDoc.ShortDesc
}
func (b BatseeCmd) Docs() *cli.CommandDocumentation {
	return cli.NewCommandDocumentation(batseeDoc, b.ArgParser())
}

func (b BatseeCmd) Hidden() bool {
	return true
}

func (b BatseeCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs(b.Name())
	ap.SupportsUint("threads", "t", "processes", "Number of tests to execute in parallel. Defaults to 12")
	return ap
}

func (b BatseeCmd) RequiresRepo() bool {
	return false
}

type batsResult struct {
	runtime time.Duration
	path    string
	err     error
}

func (b BatseeCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	apr, err := b.ArgParser().Parse(args)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), nil)
	}
	threads, hasThreads := apr.GetUint("threads")
	if !hasThreads {
		threads = 12
	}

	// Get the current working directory
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current working directory:", err)
		return 1
	}

	// This is pretty restrictive. Loosen this up.
	if filepath.Base(cwd) != "bats" || filepath.Base(filepath.Dir(cwd)) != "integration-tests" {
		cli.Println("Current working directory is not integration-tests/bats")
		return 1
	}

	// Get a list of all files in this directory which end in ".bats"
	files, err := ioutil.ReadDir(cwd)
	if err != nil {
		cli.Println("Error reading directory:", err)
		return 1
	}
	workQueue := []string{}
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".bats" {
			workQueue = append(workQueue, file.Name())
		}
	}

	jobs := make(chan string, len(workQueue))
	results := make(chan batsResult, len(workQueue))

	var wg sync.WaitGroup
	for i := uint64(0); i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(jobs, results)
		}()
	}

	for _, job := range workQueue {
		jobs <- job
	}
	close(jobs)

	cli.Println(fmt.Sprintf("Waiting for workers (%d) to finish", threads))

	wg.Wait()
	close(results)

	exitStatus := 0
	for result := range results {
		cli.Println(fmt.Sprintf("Test %s completed in %s", result.path, result.runtime.String()))
	}
	return exitStatus
}

func worker(jobs <-chan string, results chan<- batsResult) {
	// Process the job and send the result to the results channel
	for job := range jobs {
		runBats(job, results)
	}
}

// runBats runs a single bats test and sends the result to the results channel. Stdout and stderr are written to files
// in the batsee_results directory in the CWD, and the error is written to the result.err field.
func runBats(path string, resultChan chan<- batsResult) {
	cmd := exec.Command("bats", path)

	result := batsResult{path: path}

	startTime := time.Now()

	outPath := fmt.Sprintf("batsee_results/%s.stdout.log", path)
	output, err := os.Create(outPath)
	if err != nil {
		cli.Println("Error creating stdout log:", err.Error())
		result.err = err
	}
	defer output.Close()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cli.Println("Error creating stdout pipe:", err.Error())
		result.err = err
	}

	errPath := fmt.Sprintf("batsee_results/%s.stderr.log", path)
	errput, err := os.Create(errPath)
	if err != nil {
		cli.Println("Error creating stderr log:", err.Error())
		result.err = err
	}
	defer errput.Close()

	stderr, err := cmd.StderrPipe()
	if err != nil {
		cli.Println("Error creating stderr pipe:", err.Error())
		result.err = err
	}

	err = cmd.Start()
	if err != nil {
		cli.Println("Error starting command:", err.Error())
		result.err = err
	}

	// do this as a goroutine so that we can tail the output files while tests are running.
	go io.Copy(output, stdout)
	go io.Copy(errput, stderr)

	err = cmd.Wait()
	if err != nil {
		// command completed with a non-0 exit code. This is "normal", so not writing to output. It will be captured
		// as part of the summary.
		result.err = err
	}

	result.runtime = time.Since(startTime)
	resultChan <- result
	return
}
