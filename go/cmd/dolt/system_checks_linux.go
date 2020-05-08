// Copyright 2019 Liquidata, Inc.
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

package main

import (
	"io/ioutil"
	"os"

	"github.com/fatih/color"
	"golang.org/x/sys/unix"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
)

func warnIfMaxFilesTooLow() {
	var lim unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &lim); err != nil {
		return
	}
	lim.Cur = lim.Max
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &lim); err != nil {
		return
	}
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &lim); err != nil {
		return
	}
}

func warnIfTmpDirMoveFails() {
	const testfile = "./testfile"

	f, err := ioutil.TempFile("", "")

	if err != nil {
		cli.PrintErrln(color.YellowString("WARNING"))
		cli.PrintErrln(color.YellowString("Failed to create temporary file: %s", err.Error()))
		return
	}

	name := f.Name()
	err = f.Close()

	if err != nil {
		cli.PrintErrln(color.YellowString("WARNING"))
		cli.PrintErrln(color.YellowString("Failed to create temporary file: %s", err.Error()))
	}

	err = os.Rename(name, testfile)

	if err != nil {
		_ = os.Remove(name)

		cli.PrintErrln(color.YellowString("WARNING"))
		cli.PrintErrln(color.YellowString("Failed to move the temporary file %s to the current directory."))
		cli.PrintErrln(color.YellowString("This is likely due to your temp directory not being located on the same volume as the current directory."))
		cli.PrintErrln(color.YellowString("While we tackle this issue you can change your TMPDIR to be a directory on the current volume using the env var TMPDIR"))
		cli.PrintErrln(color.YellowString("Visit https://github.com/liquidata-inc/dolt/issues/253 to find the state of this issue."))
		return
	}

	_ = os.Remove(testfile)
}
