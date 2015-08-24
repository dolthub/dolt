package main

import (
	"flag"

	"github.com/attic-labs/noms/chunks"
)

var (
	port = flag.Int("port", 8000, "")
)

func main() {
	flags := chunks.NewFlags()
	flag.Parse()
	cs := flags.CreateStore()
	if cs == nil {
		flag.Usage()
		return
	}

	server := chunks.NewHttpStoreServer(cs, *port)
	server.Run()
}
