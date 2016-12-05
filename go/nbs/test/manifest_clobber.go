// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"flag"
	"log"
	"os"

	"golang.org/x/sys/unix"
)

func main() {
	flag.Parse()

	if flag.NArg() < 3 {
		log.Fatalln("Not enough arguments")
	}

	l, err := os.Create(flag.Arg(0))
	if err != nil {
		log.Fatalln(err)
	}
	defer l.Close()
	// lock released by closing l.
	err = unix.Flock(int(l.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err == unix.EWOULDBLOCK {
		return
	}
	if err != nil {
		log.Fatalln(err)
	}

	// Clobber manifest file at flag.Arg(1) with contents at flag.Arg(2)
	m, err := os.Create(flag.Arg(1))
	if err != nil {
		log.Fatalln(err)
	}
	defer m.Close()
	if _, err = m.WriteString(flag.Arg(2)); err != nil {
		log.Fatalln(err)
	}
}
