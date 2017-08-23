// +build darwin linux netbsd openbsd

package main

import (
	"fmt"
	"syscall"
)

func init() {
	fileDescriptorCheck = checkAndSetUlimit
}

func checkAndSetUlimit() error {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return fmt.Errorf("error getting rlimit: %s", err)
	}

	var setting bool
	if rLimit.Cur < ipfsFileDescNum {
		if rLimit.Max < ipfsFileDescNum {
			log.Error("adjusting max")
			rLimit.Max = ipfsFileDescNum
		}
		fmt.Printf("Adjusting current ulimit to %d...\n", ipfsFileDescNum)
		rLimit.Cur = ipfsFileDescNum
		setting = true
	}

	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return fmt.Errorf("error setting ulimit: %s", err)
	}

	if setting {
		fmt.Printf("Successfully raised file descriptor limit to %d.\n", ipfsFileDescNum)
	}

	return nil
}
