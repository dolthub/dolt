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
	"strings"
	"sync"
	"syscall"
	"time"
)

var batseeDoc = cli.CommandDocumentationContent{
	ShortDesc: `Run the Bats Tests concurrently`,
	LongDesc:  `From within the integration-test/bats directory, run the bats tests concurrently. Output for each test is written to a file in the batsee_output directory.`,
	Synopsis: []string{
		`-t 42`,
		`--skip-slow --max-time 1h15m`,
		`--retries 2 --only types.bats,foreign-keys.bats`,
	},
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
	ap := argparser.NewArgParserWithMaxArgs(b.Name(), 0)
	ap.SupportsUint("threads", "t", "threads", "Number of tests to execute in parallel. Defaults to 12")
	ap.SupportsFlag("skip-slow", "s", "Skip slow tests. This is a static list of test we know are slow, may grow stale.")
	ap.SupportsString("max-time", "", "", "Maximum time to run tests. Defaults to 30m")
	ap.SupportsString("only", "", "", "Only run the specified test, or tests (comma separated)")
	ap.SupportsInt("retries", "r", "retries", "Number of times to retry a failed test. Defaults to 1")
	return ap
}

func (b BatseeCmd) RequiresRepo() bool {
	return false
}

type batsResult struct {
	runtime time.Duration
	path    string
	err     error
	skipped bool
	aborted bool
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
	ap := b.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, batseeDoc, ap))

	apr, err := ap.Parse(args)
	if err != nil {
		if err != argparser.ErrHelp {
			verr := errhand.NewDError("", "", err, true)
			return HandleVErrAndExitCode(verr, usage)
		}
		help()
		return 0
	}

	threads, hasThreads := apr.GetUint("threads")
	if !hasThreads {
		threads = 12
	}

	durationInput, hasDuration := apr.GetValue("max-time")
	if !hasDuration {
		durationInput = "30m"
	}
	duration, err := time.ParseDuration(durationInput)
	if err != nil {
		cli.Println("Error parsing duration:", err)
		return 1
	}

	skipSlow := false
	_, hasVal := apr.GetValue("skip-slow")
	if hasVal {
		skipSlow = true
	}

	limitTo := map[string]bool{}
	runOnlyStr, hasRunOnly := apr.GetValue("only")
	if hasRunOnly {
		// split runOnlyStr on commas
		for _, test := range strings.Split(runOnlyStr, ",") {
			test = strings.TrimSpace(test)
			limitTo[test] = true
		}
	}

	retries, hasRetries := apr.GetInt("retries")
	if !hasRetries {
		retries = 1
	}

	startTime := time.Now()

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
		if !skipSlow {
			workQueue = append(workQueue, key)
		}
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

	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	var wg sync.WaitGroup
	for i := uint64(0); i < threads; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()
			worker(jobs, retries, results, ctx, limitTo)
		}()
	}

	for _, job := range workQueue {
		jobs <- job
	}
	close(jobs)

	cli.Println(fmt.Sprintf("Waiting for workers (%d) to finish", threads))
	comprehensiveWait(ctx, &wg)

	close(results)

	exitStatus := printResults(results)
	cli.Println(fmt.Sprintf("BATS Executor Exemplar completed in: %s with a status of %d", durationStr(time.Since(startTime)), exitStatus))
	return exitStatus
}

func comprehensiveWait(ctx context.Context, wg *sync.WaitGroup) {
	wgChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(wgChan)
	}()

	prematureExit := false
	select {
	case <-ctx.Done():
		prematureExit = true
		break
	case <-wgChan:
	}

	if prematureExit {
		// Still need to wait for workers to finish. They got the signal, but we will panic if we don't let them finish.
		<-wgChan
	}
}

func printResults(results <-chan batsResult) int {

	passStr := color.GreenString(fmt.Sprintf("%20s", "PASSED"))
	failStr := color.RedString(fmt.Sprintf("%20s", "FAILED"))
	skippedStr := color.YellowString(fmt.Sprintf("%20s", "SKIPPED"))
	skippedNoTimeStr := color.YellowString(fmt.Sprintf("%20s", "SKIPPED (no time)"))
	terminatedStr := color.RedString(fmt.Sprintf("%20s", "TERMINATED"))

	failedQ := []batsResult{}
	skippedQ := []batsResult{}
	for result := range results {
		if result.skipped {
			skippedQ = append(skippedQ, result)
			continue
		}

		if result.err != nil {
			failedQ = append(failedQ, result)
		} else {
			cli.Println(fmt.Sprintf("%s %-40s (time: %s)", passStr, result.path, durationStr(result.runtime)))
		}
	}
	for _, result := range skippedQ {
		reason := skippedStr
		if result.aborted {
			reason = skippedNoTimeStr
		}
		cli.Println(fmt.Sprintf("%s %-40s (time:NA)", reason, result.path))
	}

	exitStatus := 0
	for _, result := range failedQ {
		reason := failStr
		if result.aborted {
			reason = terminatedStr
		}
		cli.Println(fmt.Sprintf("%s %-40s (time:%s)", reason, result.path, durationStr(result.runtime)))
		exitStatus = 1
	}
	return exitStatus
}

func durationStr(duration time.Duration) string {
	return fmt.Sprintf("%02d:%02d", int(duration.Minutes()), int(duration.Seconds())%60)
}

func worker(jobs <-chan string, retries int, results chan<- batsResult, ctx context.Context, limitTo map[string]bool) {
	for job := range jobs {
		runBats(job, retries, results, ctx, limitTo)
	}
}

// runBats runs a single bats test and sends the result to the results channel. Stdout and stderr are written to files
// in the batsee_results directory in the CWD, and the error is written to the result.err field.
func runBats(path string, retries int, resultChan chan<- batsResult, ctx context.Context, limitTo map[string]bool) {
	cmd := exec.CommandContext(ctx, "bats", path)
	// Set the process group ID so that we can kill the entire process tree if it runs too long. We need to differenciate
	// process group of the sub process from this one, because kill the primary process if we don't.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = append(os.Environ(), fmt.Sprintf("DOLT_TEST_RETRIES=%d", retries))

	result := batsResult{path: path}

	if limitTo != nil && len(limitTo) != 0 && !limitTo[path] {
		result.skipped = true
		resultChan <- result
		return
	}

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

	if result.err == nil {
		// All systems go!
		err = cmd.Start()
		if err != nil {
			if ctx.Err() == context.DeadlineExceeded || ctx.Err() == context.Canceled {
				result.aborted = true
				result.skipped = true
			} else {
				cli.Println("Error starting command:", err.Error())
			}
			result.err = err
		}
	}

	if cmd.Process != nil {
		// Process started. Now we may have things to clean up if things go sideways.
		// do this as a goroutines so that we can tail the output files while tests are running.
		go io.Copy(output, stdout)
		go io.Copy(errput, stderr)
		pgroup := -1 * cmd.Process.Pid

		err = cmd.Wait()
		if err != nil {
			if ctx.Err() == context.DeadlineExceeded || ctx.Err() == context.Canceled {
				// Kill entire process group with fire
				syscall.Kill(pgroup, syscall.SIGKILL)
				result.aborted = true
			}
			// command completed with a non-0 exit code. This is "normal", so not writing to output. It will be captured
			// as part of the summary.
			result.err = err
		}
	}
	result.runtime = time.Since(startTime)
	resultChan <- result
	return
}
