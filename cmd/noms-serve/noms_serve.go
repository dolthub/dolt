package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/attic-labs/noms/clients/go/flags"
	"github.com/attic-labs/noms/clients/go/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
)

var (
	port        = flag.Int("port", 8000, "")
	stdoutIsTty = flag.Int("stdout-is-tty", -1, "value of 1 forces tty ouput, 0 forces non-tty output (provided for use by other programs)")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Serves a Noms database over HTTP\n")
		fmt.Fprintln(os.Stderr, "Usage: noms serve <database>")
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
		if util.MaybeStartCPUProfile() {
			defer util.StopCPUProfile()
		}
		server.Run()
	})
}
