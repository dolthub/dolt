package mcp

type Server interface {}

type serverImpl struct {
	db Database
}

var _ Server = &serverImpl{}

func NewMCPServer() Server {
	return &serverImpl{}
}

