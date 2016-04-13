package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
)

type flags struct {
	ldb    chunks.LevelDBStoreFlags
	dynamo chunks.DynamoStoreFlags
	memory chunks.MemoryStoreFlags
}

var (
	port = flag.Int("port", 8000, "")
)

func main() {
	f := flags{
		chunks.LevelDBFlags(""),
		chunks.DynamoFlags(""),
		chunks.MemoryFlags(""),
	}
	flag.Parse()

	var cf chunks.Factory
	if cf = f.ldb.CreateFactory(); cf != nil {
	} else if cf = f.dynamo.CreateFactory(); cf != nil {
	} else if cf = f.memory.CreateFactory(); cf != nil {
	}

	if cf == nil {
		flag.Usage()
		return
	}

	server := datas.NewRemoteDataStoreServer(cf, *port)

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
	cf.Shutter()
}
