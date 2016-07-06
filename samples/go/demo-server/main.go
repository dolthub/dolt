// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"flag"
	"fmt"

	"github.com/attic-labs/noms/go/chunks"
)

var (
	portFlag    = flag.Int("port", 8000, "port to listen on")
	ldbDir      = flag.String("ldb-dir", "", "directory for ldb database")
	authKeyFlag = flag.String("authkey", "", "token to use for authenticating write operations")
)

func usage() {
	fmt.Println("Usage: demo-server -authkey <authkey> [options]")
	flag.PrintDefaults()
}

func main() {
	chunks.RegisterLevelDBFlags(flag.CommandLine)
	dynFlags := chunks.DynamoFlags("")

	flag.Usage = usage
	flag.Parse()

	if *portFlag == 0 || *authKeyFlag == "" {
		usage()
		return
	}

	var factory chunks.Factory
	if factory = dynFlags.CreateFactory(); factory != nil {
		fmt.Printf("Using dynamo ...\n")
	} else if *ldbDir != "" {
		factory = chunks.NewLevelDBStoreFactoryUseFlags(*ldbDir)
		fmt.Printf("Using leveldb ...\n")
	} else {
		factory = chunks.NewMemoryStoreFactory()
		fmt.Printf("Using mem ...\n")
	}
	defer factory.Shutter()

	startWebServer(factory, *authKeyFlag)
}
