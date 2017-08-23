package main

import (
	"os"
	"strconv"
)

var ipfsFileDescNum = uint64(2048)

func init() {
	if val := os.Getenv("IPFS_FD_MAX"); val != "" {
		n, err := strconv.Atoi(val)
		if err != nil {
			log.Errorf("bad value for IPFS_FD_MAX: %s", err)
		} else {
			ipfsFileDescNum = uint64(n)
		}
	}
}
