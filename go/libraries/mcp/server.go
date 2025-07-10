package mcp

import (
	"fmt"
	"github.com/mark3labs/mcp-go/server"
)

const (
	DoltMCPServerName = "dolt-mcp"
	DoltMCPServerVersion = "0.1.0"
)

type Server interface {}

type serverImpl struct {
	db Database
	mcp *server.MCPServer
}

var _ Server = &serverImpl{}

func NewMCPServer(config Config) (Server, error) {
	fmt.Println("DUSTIN: validating config")
	err := config.Validate()
	if err != nil {
		return nil, err
	}
	fmt.Println("DUSTIN: config is valid")
	db, err := NewDatabase(config)
	if err != nil {
		return nil, err
	}
	mcp := server.NewMCPServer(DoltMCPServerName, DoltMCPServerVersion)
	return &serverImpl{
		mcp: mcp,
		db: db,
	}, nil
}

