// +build darwin freebsd dragonfly netbsd openbsd

package reuseport

import (
	"syscall"
)

var soReusePort = syscall.SO_REUSEPORT
var soReuseAddr = syscall.SO_REUSEADDR
