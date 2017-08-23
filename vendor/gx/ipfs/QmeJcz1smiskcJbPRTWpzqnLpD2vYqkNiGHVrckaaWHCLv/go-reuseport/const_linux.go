// +build linux

package reuseport

import (
	"syscall"
)

var soReusePort = 15 // this is not defined in unix go pkg.
var soReuseAddr = syscall.SO_REUSEADDR
