// +build !windows

package iptbutil

import (
	"os/exec"
	"syscall"
)

func setupOpt(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}
