package main

import (
	"fmt"
	"github.com/fatih/color"
	"golang.org/x/sys/unix"

	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
)

var red = color.New(color.FgRed).SprintFunc()
var yellow = color.New(color.FgYellow).SprintFunc()

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
		cli.Printf("%s\n", yellow("WARNING"))
		cli.Printf("%s\n", yellow(fmt.Sprintf("Only %d file descriptors are available for this process, which is less than the recommended amount, %d.", lim.Cur, warningThreshold)))
		cli.Printf("%s\n", yellow("You may experience I/O errors by continuing to run dolt in this configuration."))
		cli.Printf("\n")
	}
}
