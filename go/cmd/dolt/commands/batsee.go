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
	"github.com/fatih/color"
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

// list of slow commands. These tend to run more than 5-7 min, so we want to run them first.
var slowCommands = map[string]bool{
	"types.bats":                 true,
	"keyless.bats":               true,
	"index-on-writes.bats":       true,
	"constraint-violations.bats": true,
	"foreign-keys.bats":          true,
	"index.bats":                 true,
	"sql-server.bats":            true,
	"index-on-writes-2.bats":     true,
	"sql.bats":                   true,
	"remotes.bats":               true,
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

	cwd, err := os.Getwd()
	if err != nil {
		cli.Println("Error getting current working directory:", err)
		return 1
	}
	// This is pretty restrictive. Loosen this up. TODO
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
	// Insert the slow tests first
	for key, _ := range slowCommands {
		workQueue = append(workQueue, key)
	}
	// Then insert the rest of the tests
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".bats" {
			if _, ok := slowCommands[file.Name()]; !ok {
				workQueue = append(workQueue, file.Name())
			}
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

	passStr := color.GreenString("PASS")
	failStr := color.RedString("FAIL")
	failedQ := []batsResult{}
	for result := range results {
		if result.err != nil {
			failedQ = append(failedQ, result)
		} else {
			cli.Println(fmt.Sprintf("%s completed in %s with status %s", result.path, durationStr(result.runtime), passStr))
		}
	}
	exitStatus := 0
	for _, result := range failedQ {
		cli.Println(fmt.Sprintf("%s completed in %s with status %s", result.path, durationStr(result.runtime), failStr))
		exitStatus = 1
	}

	return exitStatus
}

func durationStr(duration time.Duration) string {
	return fmt.Sprintf("%02d:%02d", int(duration.Minutes()), int(duration.Seconds())%60)
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
