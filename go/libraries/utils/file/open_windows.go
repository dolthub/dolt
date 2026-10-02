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

//go:build windows
// +build windows

package file

import (
	"errors"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
)

const (
	maxRetryWait        = 10000 * time.Millisecond
	retryWaitMultiplier = 10
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

// MoveDir moves a directory from |oldpath| to |newpath| on Windows
// without replacing an existing destination. It retries locked or
// access-denied moves, but stops retrying as soon as the destination
// path exists and returns an error wrapping [os.ErrExist].
func MoveDir(oldpath, newpath string) error {
	from, err := syscall.UTF16PtrFromString(oldpath)
	if err != nil {
		return &os.LinkError{Op: "MoveDir", Old: oldpath, New: newpath, Err: err}
	}
	to, err := syscall.UTF16PtrFromString(newpath)
	if err != nil {
		return &os.LinkError{Op: "MoveDir", Old: oldpath, New: newpath, Err: err}
	}

	err = windows.MoveFileEx(from, to, 0)
	if isMoveRetryable(err) {
		if _, statErr := os.Lstat(newpath); statErr == nil {
			linkErr := &os.LinkError{Op: "MoveDir", Old: oldpath, New: newpath, Err: err}
			return errors.Join(os.ErrExist, linkErr)
		}
		for waitTime := time.Millisecond; isMoveRetryable(err) && waitTime <= maxRetryWait; waitTime *= retryWaitMultiplier {
			time.Sleep(waitTime)
			err = windows.MoveFileEx(from, to, 0)
			if err != nil {
				if _, statErr := os.Lstat(newpath); statErr == nil {
					linkErr := &os.LinkError{Op: "MoveDir", Old: oldpath, New: newpath, Err: err}
					return errors.Join(os.ErrExist, linkErr)
				}
			}
		}
	}
	if err != nil {
		linkErr := &os.LinkError{Op: "MoveDir", Old: oldpath, New: newpath, Err: err}
		if _, statErr := os.Lstat(newpath); statErr == nil {
			return errors.Join(os.ErrExist, linkErr)
		}
		return linkErr
	}
	return nil
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

func isMoveRetryable(err error) bool {
	return isAccessError(err) || isSharingViolation(err)
}

func isSharingViolation(err error) bool {
	if err == nil {
		return false
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno == windows.ERROR_SHARING_VIOLATION || errno == windows.ERROR_LOCK_VIOLATION
	}
	return false
}
