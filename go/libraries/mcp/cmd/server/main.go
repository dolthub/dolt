package main

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/mcp"
)

func main() {
	middleware := []mcp.Middleware{
		mcp.NoopAuthenticationHTTPMiddleware,
	}
	srv, err := mcp.NewMCPServer(middleware)
	if err != nil {
		panic(err)
	}
	srv.RegisterTools()
	srv.ListenAndServe(context.Background(), 8080)
}

