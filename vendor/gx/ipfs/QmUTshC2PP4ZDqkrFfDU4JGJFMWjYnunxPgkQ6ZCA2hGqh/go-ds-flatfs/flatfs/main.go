package main

import (
	"fmt"
	"os"
	"strconv"

	"gx/ipfs/QmUTshC2PP4ZDqkrFfDU4JGJFMWjYnunxPgkQ6ZCA2hGqh/go-ds-flatfs"
)

// To convert from the old format to a new format with a different
// sharding function use:
//   flatfs upgrade blocks 5
//   flatfs create blocks-new v1/next-to-last/2
//   flatfs move blocks blocks-new
//   rmdir blocks
//   mv blocks-new blocks
// to do the reverse
//   flatfs create blocks-new v1/prefix/5
//   flatfs move blocks blocks-new
//   rmdir blocks
//   mv blocks-new blocks
//   flatfs downgrade blocks

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s create DIR SHARDFUN | upgrade DIR PREFIXLEN | downgrade DIR | move OLDDIR NEWDIR\n", os.Args[0])
	os.Exit(1)
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "%v\n", err)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	switch os.Args[1] {
	case "create":
		if len(os.Args) != 4 {
			usage()
		}
		dir := os.Args[2]
		funStr := os.Args[3]
		if funStr[0] != '/' {
			if funStr[0] != 'v' { // and version if not provided
				funStr = "v1/" + funStr
			}
			funStr = flatfs.PREFIX + funStr
		}
		fun, err := flatfs.ParseShardFunc(funStr)
		if err != nil {
			fail(err)
		}
		err = flatfs.Create(dir, fun)
		if err != nil {
			fail(err)
		}
	case "upgrade":
		if len(os.Args) != 4 {
			usage()
		}
		dir := os.Args[2]
		prefixLen, err := strconv.Atoi(os.Args[3])
		if err != nil {
			fail(err)
		}
		err = flatfs.UpgradeV0toV1(dir, prefixLen)
		if err != nil {
			fail(err)
		}
	case "downgrade":
		if len(os.Args) != 3 {
			usage()
		}
		dir := os.Args[2]
		err := flatfs.DowngradeV1toV0(dir)
		if err != nil {
			fail(err)
		}
	case "move":
		if len(os.Args) != 4 {
			usage()
		}
		oldDir := os.Args[2]
		newDir := os.Args[3]
		err := flatfs.Move(oldDir, newDir, os.Stderr)
		if err != nil {
			fail(err)
		}
	default:
		usage()
	}
}
