package osh

import "runtime"

func IsUnix() bool {
	switch runtime.GOOS {
	case "android", "darvin", "dragonfly", "freebsd", "linux", "netbsd", "solaris":
		return true
	default:
		return false
	}
}
