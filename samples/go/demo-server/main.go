// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	flag "github.com/juju/gnuflag"
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
	flag.Parse(true)

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
	factory = &cachingReadThroughStoreFactory{chunks.NewMemoryStore(), factory}
	defer factory.Shutter()

	startWebServer(factory, *authKeyFlag)
}

type cachingReadThroughStoreFactory struct {
	cache   *chunks.MemoryStore
	factory chunks.Factory
}

func (f *cachingReadThroughStoreFactory) CreateStore(ns string) chunks.ChunkStore {
	d.PanicIfFalse(f.factory != nil, "Cannot use cachingReadThroughStoreFactory after Shutter().")
	return chunks.NewReadThroughStore(f.cache, f.factory.CreateStore(ns))
}

func (f *cachingReadThroughStoreFactory) Shutter() {
	f.factory.Shutter()
	f.factory = nil
	f.cache.Close()
}
