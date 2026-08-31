// Copyright 2026 Dolthub, Inc.
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

package sqlserver

import (
	"errors"
	"os"
	"syscall"
)

// ProcessExists reports whether a process with pid is running.
//
// As documented by [os.FindProcess], on Unix systems FindProcess
// constructs an [os.Process] struct without verifying existence.
// Liveness is tested by sending signal 0 via [os.Process.Signal]
// using the [POSIX.1-2017 kill] convention.
//
// [POSIX.1-2017 kill]: https://pubs.opengroup.org/onlinepubs/9699919799/functions/kill.html
func ProcessExists(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = p.Signal(syscall.Signal(0))
	if err == nil {
		return true
	}
	if errors.Is(err, syscall.EPERM) {
		return true
	}
	return false
}
