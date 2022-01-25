// Copyright 2019-2022 Dolthub, Inc.
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

package dolt_builder

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"

	"golang.org/x/sync/errgroup"
)

func Run(commitList []string) error {
	parentCtx := context.Background()

	doltBin, err := getDoltBin()
	if err != nil {
		return err
	}

	// check for git on path
	err = GitVersion(parentCtx)
	if err != nil {
		return err
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	// make temp dir for cloning/copying dolt source
	tempDir := filepath.Join(cwd, "clones-copies")
	err = os.MkdirAll(tempDir, os.ModePerm)
	if err != nil {
		return err
	}

	// clone dolt source
	err = GitCloneBare(parentCtx, tempDir)
	if err != nil {
		return err
	}

	repoDir := filepath.Join(tempDir, "dolt.git")

	withKeyCtx, cancel := context.WithCancel(parentCtx)
	g, ctx := errgroup.WithContext(withKeyCtx)

	// handle user interrupt
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-quit
		defer wg.Done()
		signal.Stop(quit)
		cancel()
	}()

	for _, commit := range commitList {
		commit := commit // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			return buildBinaries(ctx, tempDir, repoDir, doltBin, commit)
		})
	}

	builderr := g.Wait()
	close(quit)
	wg.Wait()

	// remove clones-copies after all go routines complete
	// will exit successfully if removal fails
	if err := os.RemoveAll(tempDir); err != nil {
		fmt.Printf("WARN: %s was not removed\n", tempDir)
		fmt.Printf("WARN: error: %v\n", err)
	}

	if builderr != nil {
		return builderr
	}

	return nil
}

// getDoltBin creates and returns the absolute path for DOLT_BIN
// if it was found, otherwise uses the current working directory
// as the parent directory for a `doltBin` directory
func getDoltBin() (string, error) {
	var doltBin string
	dir := os.Getenv("DOLT_BIN")
	if dir == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		doltBin = filepath.Join(cwd, "doltBin")
	} else {
		abs, err := filepath.Abs(dir)
		if err != nil {
			return "", err
		}
		doltBin = abs
	}
	err := os.MkdirAll(doltBin, os.ModePerm)
	if err != nil {
		return "", err
	}
	return doltBin, nil
}

// buildBinaries builds a dolt binary at the given commit and stores it in the doltBin
func buildBinaries(ctx context.Context, tempDir, repoDir, doltBinDir, commit string) error {
	checkoutDir := filepath.Join(tempDir, commit)
	if err := os.MkdirAll(checkoutDir, os.ModePerm); err != nil {
		return err
	}

	err := GitCheckoutTree(ctx, repoDir, checkoutDir, commit)
	if err != nil {
		return err
	}

	commitDir := filepath.Join(doltBinDir, commit)
	if err := os.MkdirAll(commitDir, os.ModePerm); err != nil {
		return err
	}

	command, err := goBuild(ctx, checkoutDir, commitDir)
	if err != nil {
		return err
	}

	return doltVersion(ctx, commitDir, command)
}

// goBuild builds the dolt binary and returns the filename
func goBuild(ctx context.Context, source, dest string) (string, error) {
	goDir := filepath.Join(source, "go")
	doltFileName := "dolt"
	if runtime.GOOS == "windows" {
		doltFileName = "dolt.exe"
	}
	toBuild := filepath.Join(dest, doltFileName)
	build := ExecCommand(ctx, "go", "build", "-o", toBuild, filepath.Join(goDir, "cmd", "dolt"))
	build.Dir = goDir
	err := build.Run()
	if err != nil {
		return "", err
	}
	return toBuild, nil
}

// doltVersion prints dolt version of binary
func doltVersion(ctx context.Context, dir, command string) error {
	doltVersion := ExecCommand(ctx, command, "version")
	doltVersion.Stderr = os.Stderr
	doltVersion.Stdout = os.Stdout
	doltVersion.Dir = dir
	return doltVersion.Run()
}
