// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/util/receipts"
	flag "github.com/juju/gnuflag"
)

var (
	portFlag       = flag.Int("port", 8000, "port to listen on")
	ldbDir         = flag.String("ldb-dir", "", "directory for ldb database")
	authKeyFlag    = flag.String("authkey", "", "token to use for authenticating write operations")
	receiptKeyFlag = flag.String("receiptkey", "", "Receipt key to use for generating and verifying receipts (generate with tools/crypto/receiptkey)")
)

func main() {
	chunks.RegisterLevelDBFlags(flag.CommandLine)
	dynFlags := chunks.DynamoFlags("")

	flag.Usage = func() {
		fmt.Println("Usage: demo-server --authkey <authkey> [options]")
		flag.PrintDefaults()
	}
	flag.Parse(true)

	if *authKeyFlag == "" {
		flag.Usage()
		os.Exit(1)
	}

	var receiptKey receipts.Key
	if *receiptKeyFlag != "" {
		var err error
		receiptKey, err = receipts.DecodeKey(*receiptKeyFlag)
		if err != nil {
			fmt.Printf("Invalid receipt key: %s\n", err.Error())
			os.Exit(1)
		}
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

	startWebServer(factory, *authKeyFlag, receiptKey)
}
