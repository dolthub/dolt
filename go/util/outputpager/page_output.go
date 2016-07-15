// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package outputpager

import (
	"flag"
	"os"
	"os/exec"

	"github.com/attic-labs/noms/go/d"
	goisatty "github.com/mattn/go-isatty"
)

var (
	noPager         bool
	flagsRegistered = false
)

func RegisterOutputpagerFlags(flags *flag.FlagSet) {
	if !flagsRegistered {
		flagsRegistered = true
		flags.BoolVar(&noPager, "no-pager", false, "suppress paging functionality")
	}
}

func PageOutput() <-chan struct{} {
	if noPager || !IsStdoutTty() {
		return nil
	}

	lessExecutable, err := exec.LookPath("less")
	d.Chk.NoError(err, "unable to find 'less' utility: %s", err)

	lessStdin, newStdout, err := os.Pipe()
	d.Chk.NoError(err, "os.Pipe() failed: %s\n", err)

	cmd := exec.Command(lessExecutable, []string{"-FSRX"}...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	os.Stdout = newStdout
	cmd.Stdin = lessStdin

	err = cmd.Start()
	d.Chk.NoError(err, "cmd execution failed: %s\n", err)

	ch := make(chan struct{})
	go func() {
		err := cmd.Wait()
		d.Chk.NoError(err, "pager exited with error: %s\n", err)
		os.Stdout.Close()
		ch <- struct{}{}
	}()

	return ch
}

func IsStdoutTty() bool {
	return goisatty.IsTerminal(os.Stdout.Fd())
}
