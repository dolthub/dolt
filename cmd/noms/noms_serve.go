// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/util/profile"
	"github.com/attic-labs/noms/go/util/verbose"
	flag "github.com/juju/gnuflag"
)

var (
	port int
)

var nomsServe = &util.Command{
	Run:       runServe,
	UsageLine: "serve [options] <database>",
	Short:     "Serves a Noms database over HTTP",
	Long:      "See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the database argument.",
	Flags:     setupServeFlags,
	Nargs:     0,
}

func setupServeFlags() *flag.FlagSet {
	serveFlagSet := flag.NewFlagSet("serve", flag.ExitOnError)
	serveFlagSet.IntVar(&port, "port", 8000, "port to listen on for HTTP requests")
	verbose.RegisterVerboseFlags(serveFlagSet)
	profile.RegisterProfileFlags(serveFlagSet)
	return serveFlagSet
}

func runServe(args []string) int {
	cfg := config.NewResolver()
	db := ""
	if len(args) > 0 {
		db = args[0]
	}
	cs, err := cfg.GetChunkStore(db)
	d.CheckError(err)
	server := datas.NewRemoteDatabaseServer(cs, port)

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
	return 0
}
