package main

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/mcp"
)

func main() {
	srv, err := mcp.NewMCPServer(nil)
	if err != nil {
		panic(err)
	}
	srv.ListenAndServe(context.Background(), 8080)
}

