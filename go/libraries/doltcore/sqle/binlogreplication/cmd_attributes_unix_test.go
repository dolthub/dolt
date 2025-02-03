// Copyright 2025 Dolthub, Inc.
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

//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd
// +build darwin dragonfly freebsd linux netbsd openbsd

package binlogreplication

import (
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

func ApplyCmdAttributes(cmd *exec.Cmd) {
	// Nothing...
}

func StopProcess(proc *os.Process) error {
	err := proc.Signal(syscall.SIGTERM)
	if err != nil {
		return err
	}
	_, err = proc.Wait()
	return err
}

// These tests spawn child process for go compiling, dolt sql-server
// and for mysqld. We would like to clean up these child processes
// when the program exits. In general, we use *testing.T.Cleanup to
// terminate any running processes associated with the test.
//
// On a shell, when a user runs 'go test .', and then they deliver
// an interrupt, '^C', the shell delivers a SIGINT to the process
// group of the foreground process. In our case, `dolt`, `go`, and
// the default signal handler for the golang runtime (this test
// program) will all terminate the program on delivery of a SIGINT.
// `mysqld`, however, does not terminate on receiving SIGINT. Thus,
// we install a handler here, and we translate the Interrupt into
// a SIGTERM against the process group. That will get `mysqld` to
// shutdown as well.
func InstallSignalHandlers() {
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)
	go func() {
		<-interrupts
		// |mysqld| will exit on SIGTERM
		syscall.Kill(-os.Getpid(), syscall.SIGTERM)
		time.Sleep(1 * time.Second)
		// Canceling this context will cause os.Process.Kill
		// to get called on any still-running processes.
		commandCtxCancel()
		time.Sleep(1 * time.Second)
		// Redeliver SIGINT to ourselves with the default
		// signal handler restored.
		signal.Reset(os.Interrupt)
		syscall.Kill(-os.Getpid(), syscall.SIGINT)
	}()
}
