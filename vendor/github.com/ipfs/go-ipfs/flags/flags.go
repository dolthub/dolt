package flags

import (
	"os"
)

var LowMemMode bool

func init() {
	if os.Getenv("IPFS_LOW_MEM") != "" {
		LowMemMode = true
	}
}
