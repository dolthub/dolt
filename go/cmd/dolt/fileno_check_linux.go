package main

import (
	"golang.org/x/sys/unix"
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
