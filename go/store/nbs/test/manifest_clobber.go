// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"flag"
	"log"
	"os"

	"github.com/juju/fslock"
)

func main() {
	flag.Parse()

	if flag.NArg() < 3 {
		log.Fatalln("Not enough arguments")
	}

	// Clobber manifest file at flag.Arg(1) with contents at flag.Arg(2) after taking lock of file flag.Arg(0)
	lockFile := flag.Arg(0)
	manifestFile := flag.Arg(1)
	manifestContents := flag.Arg(2)

	// lock released by closing l.
	lck := fslock.New(lockFile)
	err := lck.TryLock()
	if err == fslock.ErrLocked {
		return
	}
	if err != nil {
		log.Fatalln(err)
	}

	defer lck.Unlock()

	m, err := os.Create(manifestFile)
	if err != nil {
		log.Fatalln(err)
	}
	defer m.Close()

	if _, err = m.WriteString(manifestContents); err != nil {
		log.Fatalln(err)
	}
}
