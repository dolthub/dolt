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
	if lim.Cur < warningThreshold {
		cli.Printf("%s\n", yellow("WARNING"))
		cli.Printf("%s\n", yellow(fmt.Sprintf("Only %d file descriptors are available for this process. This is less than the recommended amount of %d.", lim.Cur, warningThreshold)))
		cli.Printf("%s\n", yellow("You may experience I/O errors by continuing to run dolt in this configuration."))
		cli.Printf("\n")
		cli.Printf("%s\n", yellow("To increase the maximum number of file descriptors please run the following:"))
		cli.Printf("%s\n", yellow("    sudo "))
	}
}