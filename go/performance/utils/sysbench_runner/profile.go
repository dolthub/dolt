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

package sysbench_runner

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

// ProfileDolt profiles dolt while running the provided tests
func ProfileDolt(ctx context.Context, config *Config, serverConfig *ServerConfig) error {
	serverParams, err := serverConfig.GetServerArgs()
	if err != nil {
		return err
	}

	err = DoltVersion(ctx, serverConfig.ServerExec)
	if err != nil {
		return err
	}

	err = UpdateDoltConfig(ctx, serverConfig.ServerExec)
	if err != nil {
		return err
	}

	testRepo, err := initDoltRepo(ctx, serverConfig, config.NomsBinFormat)
	if err != nil {
		return err
	}

	tests, err := GetTests(config, serverConfig, nil)
	if err != nil {
		return err
	}

	for i := 0; i < config.Runs; i++ {
		for _, test := range tests {
			profile, err := profileTest(ctx, test, config, serverConfig, serverParams, testRepo, serverConfig.ProfilePath)
			if err != nil {
				return err
			}
			fmt.Println("DUSTIN: created profile:", profile)
		}
	}

	// todo: merge profiles together

	err = os.RemoveAll(testRepo)
	if err != nil {
		return err
	}

	return nil
}

func profileTest(ctx context.Context, test *Test, config *Config, serverConfig *ServerConfig, serverParams []string, testRepo, profileDir string) (string, error) {
	profilePath, err := os.MkdirTemp("", test.Name)
	if err != nil {
		return "", err
	}

	tempProfile := filepath.Join(profilePath, cpuProfileFilename)
	profileParams := make([]string, 0)
	profileParams = append(profileParams, profileFlag, cpuProfile, profilePathFlag, profilePath)
	profileParams = append(profileParams, serverParams...)

	withKeyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	gServer, serverCtx := errgroup.WithContext(withKeyCtx)
	server := getServer(serverCtx, serverConfig, testRepo, profileParams)

	// handle user interrupt
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		s := <-quit
		defer wg.Done()
		server.Process.Signal(s)
		signal.Stop(quit)
	}()

	// launch the dolt server
	gServer.Go(func() error {
		server.Stdout = os.Stdout
		server.Stderr = os.Stderr
		return server.Run()
	})

	// sleep to allow the server to start
	time.Sleep(5 * time.Second)

	_, err = benchmark(withKeyCtx, test, config, serverConfig, stampFunc, serverConfig.GetId())
	if err != nil {
		close(quit)
		wg.Wait()
		return "", err
	}

	// send signal to dolt server
	// must be interrupt signal to complete profile
	quit <- syscall.SIGINT

	err = gServer.Wait()
	if err != nil {
		// we expect a kill error
		// we only exit in error if this is not the
		// error
		fmt.Println("DUSTIN: wait error:", err.Error())
		if err.Error() != "signal: killed" {
			fmt.Println(err)
			close(quit)
			wg.Wait()
			return "", err
		}
	}

	fmt.Println("Successfully killed server")
	close(quit)
	wg.Wait()

	info, err := os.Stat(tempProfile)
	if err != nil {
		return "", err
	}
	if info.Size() < 1 {
		return "", fmt.Errorf("failed to create profile: file was empty")
	}

	finalProfile := filepath.Join(profileDir, fmt.Sprintf("%s_%s_%s", serverConfig.Id, test.Name, cpuProfileFilename))
	err = moveFile(tempProfile, finalProfile)
	return finalProfile, err
}

func moveFile(sourcePath, destPath string) error {
	err := copyFile(sourcePath, destPath)
	if err != nil {
		return err
	}
	return os.Remove(sourcePath)
}

func copyFile(sourcePath, destPath string) error {
	inputFile, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer inputFile.Close()
	outputFile, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer outputFile.Close()
	_, err = io.Copy(outputFile, inputFile)
	return err
}
