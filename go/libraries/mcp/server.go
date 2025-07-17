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
	GetHTTPEndpoint() (string, error) 
	GetMCPServer() *server.MCPServer
	RegisterTools()
	ListenAndServe(ctx context.Context, port int)
}

type Middleware func(http.Handler) http.Handler

type serverImpl struct {
	mcp *server.MCPServer
	handler http.Handler
	port int
}

var _ Server = &serverImpl{}

func ChainMiddleware(middleware ...Middleware) Middleware {
	return func(final http.Handler) http.Handler {
		for i := len(middleware) - 1; i >= 0; i-- {
			final = middleware[i](final)
		}
		return final
	}
}

func NewMCPServer(middleware []Middleware) (Server, error) {
	mcp := server.NewMCPServer(DoltMCPServerName, DoltMCPServerVersion)
	baseHandler := server.NewStreamableHTTPServer(mcp)

	finalHandler := ChainMiddleware(middleware...)(baseHandler)

	return &serverImpl{
		mcp:     mcp,
		handler: finalHandler,
	}, nil
}

func (s *serverImpl) GetMCPServer() *server.MCPServer {
	return s.mcp
}

func (s *serverImpl) ListenAndServe(ctx context.Context, port int) {
	s.port = port
	serve(ctx, s.handler, port)
}

func (s *serverImpl) validateCallToolRequest(ctx context.Context, request mcp.CallToolRequest) error {
	// todo: validate stuff here
	fmt.Println("DUSTIN: validating call tool request")
	return nil
}

func (s *serverImpl) getAuthenticatedClientDBConnection(ctx context.Context, request mcp.CallToolRequest) (Database, error) {
	fmt.Println("DUSTIN: getting client db connection")
	return nil, nil
}

func (s *serverImpl) registerPingTool() {
	pingTool := mcp.NewTool(PingToolName, mcp.WithDescription("Pings the Dolt server."))
	s.mcp.AddTool(pingTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if err := s.validateCallToolRequest(ctx, request); err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText("called ping"), nil
		// clientDBConn, err := s.getAuthenticatedClientDBConnection(ctx, request) 
		// if err != nil {
		// 	return mcp.NewToolResultError(err.Error()), nil
		// }
		// result, err := clientDBConn.QueryContext(ctx, PingSQLQuery)
		// if err != nil {
		// 	return mcp.NewToolResultError(err.Error()), nil
		// }
		// return mcp.NewToolResultText(result), nil
	})
}

func (s *serverImpl) registerListDatabasesTool() {
	listDatabasesTool := mcp.NewTool(ListDatabasesToolName, mcp.WithDescription("List all databases in the Dolt server"))
	s.mcp.AddTool(listDatabasesTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if err := s.validateCallToolRequest(ctx, request); err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText("called list databases"), nil
		// clientDBConn, err := s.getAuthenticatedClientDBConnection(ctx, request) 
		// if err != nil {
		// 	return mcp.NewToolResultError(err.Error()), nil
		// }
		// result, err := clientDBConn.QueryContext(ctx, ListDatabasesSQLQuery)
		// if err != nil {
		// 	return mcp.NewToolResultError(err.Error()), nil
		// }
		// return mcp.NewToolResultText(result), nil
	})
}

func (s *serverImpl) RegisterPrompts() error {
	return nil
}

func (s *serverImpl) RegisterTools() {
	s.registerPingTool()
	s.registerListDatabasesTool()
}

func (s *serverImpl) GetHTTPEndpoint() (string, error) {
	if s.port != 0 {
		return fmt.Sprintf("http://localhost:%d/mcp", s.port), nil	
	}

	return "", nil
}

func serve(ctx context.Context, handler http.Handler, port int) {
	portStr := fmt.Sprintf(":%d", port)
	srv := &http.Server{
		Addr: portStr,
		Handler: handler,
	}

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
			if err := srv.Shutdown(ctxTimeout); err != nil {
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
	fmt.Println("serving mcp server on", portStr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Println("error serving mcp server:", err.Error())
	}

	fmt.Println("mcp server stopped.")
}
