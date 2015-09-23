package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/http"
)

var (
	port = flag.Int("port", 8000, "")
)

func main() {
	flags := datas.NewFlags()
	flag.Parse()
	ds, ok := flags.CreateDataStore()
	if !ok {
		flag.Usage()
		return
	}

	server := http.NewHttpServer(ds, *port)

	// Shutdown server gracefully so that profile may be written
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		server.Stop()
		ds.Close()
	}()

	d.Try(func() {
		if util.MaybeStartCPUProfile() {
			defer util.StopCPUProfile()
		}
		server.Run()
	})
}
