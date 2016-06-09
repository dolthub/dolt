// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/util/profile"
	"github.com/attic-labs/noms/samples/go/flags"
	"github.com/attic-labs/noms/samples/go/util"
)

var (
	port = flag.Int("port", 8000, "")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Serves a Noms database over HTTP\n\n")
		fmt.Fprintf(os.Stderr, "Usage: noms serve <database>\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nFor detailed information on spelling databases, see: at https://github.com/attic-labs/noms/blob/master/doc/spelling.md.\n\n")
	}

	flags.RegisterDatabaseFlags()
	flag.Parse()

	if len(flag.Args()) != 1 {
		flag.Usage()
		return
	}

	spec, err := flags.ParseDatabaseSpec(flag.Arg(0))
	util.CheckError(err)
	if spec.Protocol != "mem" && spec.Protocol != "ldb" {
		err := errors.New("Illegal database spec for server, must be 'mem' or 'ldb'")
		util.CheckError(err)
	}
	cs, err := spec.ChunkStore()
	util.CheckError(err)

	server := datas.NewRemoteDatabaseServer(cs, *port)

	// Shutdown server gracefully so that profile may be written
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		server.Stop()
	}()

	d.Try(func() {
		defer profile.MaybeStartProfile().Stop()
		server.Run()
	})
}
