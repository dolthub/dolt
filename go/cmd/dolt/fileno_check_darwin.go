// Copyright 2019 Dolthub, Inc.
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
	"github.com/fatih/color"
	"golang.org/x/sys/unix"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
)

const warningThreshold = 4096

// Darwin setrlimit fails with EINVAL if
// lim.Cur > OPEN_MAX (from sys/syslimits.h), regardless of lim.Max.
// Just choose a reasonable number here.
const darwinMaxFiles = 8192

func warnIfMaxFilesTooLow() {
	var lim unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &lim); err != nil {
		return
	}
	lim.Cur = lim.Max
	if lim.Cur > darwinMaxFiles {
		lim.Cur = darwinMaxFiles
	}
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &lim); err != nil {
		return
	}
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &lim); err != nil {
		return
	}
	if lim.Cur < warningThreshold {
		cli.PrintErrln(color.YellowString("WARNING"))
		cli.PrintErrln(color.YellowString("Only %d file descriptors are available for this process, which is less than the recommended amount, %d.", lim.Cur, warningThreshold))
		cli.PrintErrln(color.YellowString("You may experience I/O errors by continuing to run dolt in this configuration."))
		cli.PrintErrln()
	}
}
