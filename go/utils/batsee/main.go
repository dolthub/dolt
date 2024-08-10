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

//go:build !windows
// +build !windows

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var batseeDoc = cli.CommandDocumentationContent{
	ShortDesc: `Run the Bats Tests concurrently`,
	LongDesc: `From within the integration-test/bats directory, run the bats tests concurrently.
Output for each test is written to a file in the batsee_output directory.
Example:  batsee -t 42 --max-time 1h15m -r 2 --only types.bats,foreign-keys.bats`,
	Synopsis: []string{
		`[-t threads] [-o dir] [--skip-slow] [--max-time time] [--only test1,test2,...]`,
	},
}

const (
	threadsFlag  = "threads"
	outputDir    = "output"
	skipSlowFlag = "skip-slow"
	maxTimeFlag  = "max-time"
	onlyFLag     = "only"
	retriesFLag  = "retries"
)

func buildArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs("batsee", 0)
	ap.SupportsInt(threadsFlag, "t", "threads", "Number of tests to execute in parallel. Defaults to 12")
	ap.SupportsString(outputDir, "o", "directory", "Directory to write output to. Defaults to 'batsee_results'")
	ap.SupportsFlag(skipSlowFlag, "s", "Skip slow tests. This is a static list of test we know are slow, may grow stale.")
	ap.SupportsString(maxTimeFlag, "", "duration", "Maximum time to run tests. Defaults to 30m")
	ap.SupportsString(onlyFLag, "", "", "Only run the specified test, or tests (comma separated)")
	ap.SupportsInt(retriesFLag, "r", "retries", "Number of times to retry a failed test. Defaults to 1")
	return ap
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

type config struct {
	threads  int
	output   string
	duration time.Duration
	skipSlow bool
	limitTo  map[string]bool
	retries  int
}

func buildConfig(apr *argparser.ArgParseResults) config {
	threads, hasThreads := apr.GetInt(threadsFlag)
	if !hasThreads {
		threads = 12
	}

	output, hasOutput := apr.GetValue(outputDir)
	if !hasOutput {
		output = "batsee_results"
	}

	durationInput, hasDuration := apr.GetValue(maxTimeFlag)
	if !hasDuration {
		durationInput = "30m"
	}
	duration, err := time.ParseDuration(durationInput)
	if err != nil {
		cli.Println("Error parsing duration:", err)
		os.Exit(1)
	}

	skipSlow := apr.Contains(skipSlowFlag)

	limitTo := map[string]bool{}
	runOnlyStr, hasRunOnly := apr.GetValue(onlyFLag)
	if hasRunOnly {
		for _, test := range strings.Split(runOnlyStr, ",") {
			test = strings.TrimSpace(test)
			limitTo[test] = true
		}
	}

	retries, hasRetries := apr.GetInt(retriesFLag)
	if !hasRetries {
		retries = 1
	}

	return config{
		threads:  threads,
		output:   output,
		duration: duration,
		skipSlow: skipSlow,
		limitTo:  limitTo,
		retries:  retries,
	}
}

func main() {
	ap := buildArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString("batsee", batseeDoc, ap))
	args := os.Args[1:]
	apr := cli.ParseArgsOrDie(ap, args, help)

	config := buildConfig(apr)

	startTime := time.Now()

	cwd, err := os.Getwd()
	if err != nil {
		cli.Println("Error getting current working directory:", err)
		os.Exit(1)
	}
	// This is pretty restrictive. Loosen this up. TODO
	if filepath.Base(cwd) != "bats" || filepath.Base(filepath.Dir(cwd)) != "integration-tests" {
		cli.Println("Current working directory is not integration-tests/bats")
		os.Exit(1)
	}

	// Get a list of all files in this directory which end in ".bats"
	files, err := os.ReadDir(cwd)
	if err != nil {
		cli.Println("Error reading directory:", err)
		os.Exit(1)
	}

	workQueue := []string{}
	// Insert the slow tests first
	for key, _ := range slowCommands {
		if !config.skipSlow {
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

	ctx := context.Background()
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, config.duration)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < config.threads; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()
			worker(jobs, results, ctx, config)
		}()
	}

	for _, job := range workQueue {
		jobs <- job
	}
	close(jobs)

	cli.Println(fmt.Sprintf("Waiting for workers (%d) to finish", config.threads))
	comprehensiveWait(ctx, &wg)

	close(results)

	exitStatus := printResults(results)
	cli.Println(fmt.Sprintf("BATS Executor Exemplar completed in: %s with a status of %d", durationStr(time.Since(startTime)), exitStatus))
	os.Exit(exitStatus)
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
	// Note that color control characters batch formatting, so we build these status strings all to be the same length
	// so they will produce the right results when included below.
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

func worker(jobs <-chan string, results chan<- batsResult, ctx context.Context, config config) {
	for job := range jobs {
		runBats(job, results, ctx, config)
	}
}

// runBats runs a single bats test and sends the result to the results channel. Stdout and stderr are written to files
// in the batsee_results directory in the CWD, and the error is written to the result.err field.
func runBats(path string, resultChan chan<- batsResult, ctx context.Context, cfg config) {
	cmd := exec.CommandContext(ctx, "bats", path)
	// Set the process group ID so that we can kill the entire process tree if it runs too long. We need to differentiate
	// process group of the sub process from this one, because kill the primary process if we don't.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = append(os.Environ(), fmt.Sprintf("DOLT_TEST_RETRIES=%d", cfg.retries))

	result := batsResult{path: path}

	if cfg.limitTo != nil && len(cfg.limitTo) != 0 && !cfg.limitTo[path] {
		result.skipped = true
		resultChan <- result
		return
	}

	// ensure cfg.output exists, and create it if it doesn't
	if _, err := os.Stat(cfg.output); os.IsNotExist(err) {
		err = os.Mkdir(cfg.output, 0755)
		if err != nil {
			cli.Println("Error creating output directory:", err.Error())
			result.err = err
			resultChan <- result
			return
		}
	}

	startTime := time.Now()

	outPath := fmt.Sprintf("%s/%s.stdout.log", cfg.output, path)
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

	errPath := fmt.Sprintf("%s/%s.stderr.log", cfg.output, path)
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
