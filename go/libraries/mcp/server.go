package mcp

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

const (
	DoltMCPServerName    = "dolt-mcp"
	DoltMCPServerVersion = "0.1.0"

	PingToolName = "ping"
	PingSQLQuery = "SELECT 'LIVE';"

	ListDatabasesToolName = "list_databases"
	ListDatabasesSQLQuery = "SHOW DATABASES;"
)

type Server interface {
	GetMCPServer() *server.MCPServer
	ListenAndServe(ctx context.Context, port int)
}

type serverImpl struct {
	db  Database
	mcp *server.MCPServer
}

var _ Server = &serverImpl{}

func NewMCPServer(config Config) (Server, error) {
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	db, err := NewDatabase(config)
	if err != nil {
		return nil, err
	}

	mcp := server.NewMCPServer(DoltMCPServerName, DoltMCPServerVersion)
	return &serverImpl{
		mcp: mcp,
		db:  db,
	}, nil
}

func (s *serverImpl) GetMCPServer() *server.MCPServer {
	return s.mcp
}

func (s *serverImpl) ListenAndServe(ctx context.Context, port int) {
	httpServer := server.NewStreamableHTTPServer(s.mcp)
	serve(ctx, httpServer, port)
}

func (s *serverImpl) registerPingTool() {
	pingTool := mcp.NewTool(PingToolName, mcp.WithDescription("Pings the Dolt server."))
	s.mcp.AddTool(pingTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		result, err := s.db.QueryContext(ctx, PingSQLQuery)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText(result), nil
	})
}

func (s *serverImpl) registerListDatabasesTool() {
	listDatabasesTool := mcp.NewTool(ListDatabasesToolName, mcp.WithDescription("List all databases in the Dolt server"))
	s.mcp.AddTool(listDatabasesTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		result, err := s.db.QueryContext(ctx, ListDatabasesSQLQuery)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText(result), nil
	})
}

func (s *serverImpl) RegisterPrompts() error {
	return nil
}

func (s *serverImpl) RegisterTools() {
	s.registerPingTool()
	s.registerListDatabasesTool()
}

func serve(ctx context.Context, httpServer *server.StreamableHTTPServer, port int) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(quit)

	shutdownOnce := sync.Once{}

	// Graceful shutdown logic shared by both signal and context
	shutdown := func(reason string) {
		shutdownOnce.Do(func() {
			fmt.Println("mcp server is shutting down due to:", reason)
			ctxTimeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := httpServer.Shutdown(ctxTimeout); err != nil {
				fmt.Println("failed to shutdown server:", err.Error())
			}
		})
	}

	// Listen for OS signal
	go func() {
		<-quit
		shutdown("signal")
	}()

	// Listen for context cancellation
	go func() {
		<-ctx.Done()
		shutdown("context cancellation")
	}()

	// Start the server
	portStr := fmt.Sprintf(":%d", port)
	fmt.Println("serving mcp server on", portStr)
	if err := httpServer.Start(portStr); err != nil && err != http.ErrServerClosed {
		fmt.Println("error serving mcp server:", err.Error())
	}

	fmt.Println("mcp server stopped.")
}
