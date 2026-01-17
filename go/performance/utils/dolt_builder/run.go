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
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"

	"golang.org/x/sync/errgroup"
)

const envDoltBin = "DOLT_BIN"

func Run(parentCtx context.Context, commitList []string, profilePath string) error {
	if profilePath != "" && len(commitList) > 1 {
		return errors.New("cannot build more that one binary when a profile is supplied")
	}

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
	err = GitCloneBare(parentCtx, tempDir, GithubDolt)
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
		commit := commit
		g.Go(func() error {
			return buildBinaries(ctx, tempDir, repoDir, doltBin, profilePath, commit)
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
	dir := os.Getenv(envDoltBin)
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
func buildBinaries(ctx context.Context, tempDir, repoDir, doltBinDir, profilePath, commit string) error {
	checkoutDir := filepath.Join(tempDir, commit)
	if err := os.MkdirAll(checkoutDir, os.ModePerm); err != nil {
		return fmt.Errorf("build %s: %w", commit, err)
	}

	err := GitCheckoutTree(ctx, repoDir, checkoutDir, commit)
	if err != nil {
		return fmt.Errorf("build %s: checkout failed: %w", commit, err)
	}

	commitDir := filepath.Join(doltBinDir, commit)
	if err := os.MkdirAll(commitDir, os.ModePerm); err != nil {
		return fmt.Errorf("build %s: %w", commit, err)
	}

	command, err := goBuild(ctx, checkoutDir, commitDir, profilePath)
	if err != nil {
		return fmt.Errorf("build %s: go build failed: %w", commit, err)
	}

	if err := doltVersion(ctx, commitDir, command); err != nil {
		return fmt.Errorf("build %s: dolt version failed: %w", commit, err)
	}
	return nil
}

// goBuild builds the dolt binary and returns the filename
func goBuild(ctx context.Context, source, dest, profilePath string) (string, error) {
	goDir := filepath.Join(source, "go")
	doltFileName := "dolt"
	if runtime.GOOS == "windows" {
		doltFileName = "dolt.exe"
	}

	args := make([]string, 0)
	args = append(args, "build")

	if profilePath != "" {
		args = append(args, fmt.Sprintf("-pgo=%s", profilePath))
	}

	toBuild := filepath.Join(dest, doltFileName)
	args = append(args, "-o", toBuild, filepath.Join(goDir, "cmd", "dolt"))

	build := ExecCommand(ctx, "go", args...)
	build.Dir = goDir
	err := RunCommand(build)
	if err != nil {
		return "", err
	}
	return toBuild, nil
}

// doltVersion prints dolt version of binary
func doltVersion(ctx context.Context, dir, command string) error {
	doltVersion := ExecCommand(ctx, command, "version")
	doltVersion.Dir = dir
	if Debug {
		doltVersion.Stdout = os.Stdout
		doltVersion.Stderr = os.Stderr
		return doltVersion.Run()
	}

	out, err := RunCommandOutput(doltVersion)
	if err != nil {
		return err
	}
	_, _ = os.Stdout.Write(out)
	return nil
}
