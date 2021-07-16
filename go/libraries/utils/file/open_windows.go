// Copyright 2021 Dolthub, Inc.
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

// +build windows

package file

import (
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
)

// Rename functions exactly like os.Rename, except that it retries upon failure on Windows. This "fixes" some errors
// that appear on Windows.
func Rename(oldpath, newpath string) error {
	err := os.Rename(oldpath, newpath)
	if isAccessError(err) {
		for waitTime := time.Duration(1); isAccessError(err) && waitTime <= 10000; waitTime *= 10 {
			time.Sleep(waitTime * time.Millisecond)
			err = os.Rename(oldpath, newpath)
		}
	}
	return err
}

// Remove functions exactly like os.Remove, except that it retries upon failure on Windows. This "fixes" some errors
// that appear on Windows.
func Remove(name string) error {
	err := os.Remove(name)
	if isAccessError(err) {
		for waitTime := time.Duration(1); isAccessError(err) && waitTime <= 10000; waitTime *= 10 {
			time.Sleep(waitTime * time.Millisecond)
			err = os.Remove(name)
		}
	}
	return err
}

// RemoveAll functions exactly like os.RemoveAll, except that it retries upon failure on Windows. This "fixes" some errors
// that appear on Windows.
func RemoveAll(path string) error {
	err := os.RemoveAll(path)
	if isAccessError(err) {
		for waitTime := time.Duration(1); isAccessError(err) && waitTime <= 10000; waitTime *= 10 {
			time.Sleep(waitTime * time.Millisecond)
			err = os.RemoveAll(path)
		}
	}
	return err
}

func isAccessError(err error) bool {
	switch err := err.(type) {
	case *os.LinkError:
		sysErr, ok := err.Err.(syscall.Errno)
		if ok && (sysErr == windows.ERROR_ACCESS_DENIED || sysErr == windows.ERROR_SHARING_VIOLATION) {
			return true
		}
	case *os.PathError:
		sysErr, ok := err.Err.(syscall.Errno)
		if ok && (sysErr == windows.ERROR_ACCESS_DENIED || sysErr == windows.ERROR_SHARING_VIOLATION) {
			return true
		}
	case *os.SyscallError:
		sysErr, ok := err.Err.(syscall.Errno)
		if ok && (sysErr == windows.ERROR_ACCESS_DENIED || sysErr == windows.ERROR_SHARING_VIOLATION) {
			return true
		}
	}
	return false
}
