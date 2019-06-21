// +build darwin dragonfly freebsd linux openbsd solaris netbsd

package nbs

import "os"

var mmapAlignment = int64(os.Getpagesize())
