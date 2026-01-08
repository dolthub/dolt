// Copyright 2025 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbfactory

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"time"

	"github.com/xtaci/smux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

// SSHRemoteFactory is a DBFactory implementation for creating databases backed by SSH remotes
type SSHRemoteFactory struct{}

func (fact SSHRemoteFactory) PrepareDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) error {
	return fmt.Errorf("ssh scheme cannot support this operation")
}

// CreateDB creates a database backed by an SSH remote using dolt transfer over stdin/stdout
func (fact SSHRemoteFactory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (datas.Database, types.ValueReadWriter, tree.NodeStore, error) {
	// For now, we'll use a local dolt transfer process instead of SSH
	// This allows testing the transport without actual SSH in the way
	
	
	// Extract path from URL - keep it absolute
	path := urlObj.Path
	
	// Start dolt transfer process with absolute path
	// Use the same binary that's currently running
	doltPath, err := os.Executable()
	if err != nil {
		// Fall back to "dolt" in PATH
		doltPath = "dolt"
	}
	cmd := exec.CommandContext(ctx, doltPath, "transfer", path)
	
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create stdin pipe: %w", err)
	}
	
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Capture stderr in background
	errChan := make(chan []byte, 1)
	go func() {
		errOut, _ := io.ReadAll(stderr)
		errChan <- errOut
	}()
	
	if err := cmd.Start(); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to start dolt transfer: %w", err)
	}
	
	// Monitor process in background but don't wait for it
	// The process should stay alive as long as we need it
	processDone := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		processDone <- err
		select {
		case errOut := <-errChan:
			if len(errOut) > 0 {
				fmt.Fprintf(os.Stderr, "[dolt transfer stderr]: %s\n", errOut)
			}
		default:
		}
	}()
	
	// Give the server time to start and check if it's still running
	select {
	case err := <-processDone:
		// Process exited early, this is a problem
		return nil, nil, nil, fmt.Errorf("dolt transfer exited early: %w", err)
	case <-time.After(500 * time.Millisecond):
		// Process still running, continue
	}
	
	// Create a stdio connection wrapper with buffering
	// Buffering helps prevent deadlocks and early pipe closing
	conn := &bufferedPipeConn{
		stdin:  stdin,
		stdout: stdout,
		reader: bufio.NewReader(stdout),
		writer: bufio.NewWriter(stdin),
	}
	
	// Wait a moment for the server to be ready for SMUX handshake
	time.Sleep(100 * time.Millisecond)
	
	// Create SMUX client session with default config
	session, err := smux.Client(conn, nil)
	if err != nil {
		cmd.Process.Kill()
		return nil, nil, nil, fmt.Errorf("failed to create smux session: %w", err)
	}
	// Don't close the session here - it needs to stay alive for the connection to work
	
	// Create gRPC client connection using SMUX streams
	grpcConn, err := grpc.Dial("stdio",
		grpc.WithContextDialer(func(ctx context.Context, target string) (net.Conn, error) {
			// Open a new stream for each connection
			stream, err := session.OpenStream()
			if err != nil {
				return nil, fmt.Errorf("failed to open smux stream: %w", err)
			}
			return stream, nil
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(128 * 1024 * 1024),
			grpc.MaxCallSendMsgSize(128 * 1024 * 1024),
		),
	)
	if err != nil {
		cmd.Process.Kill()
		return nil, nil, nil, fmt.Errorf("failed to create grpc client: %w", err)
	}
	
	// Create chunk store client
	client := remotesapi.NewChunkStoreServiceClient(grpcConn)
	// Use the path as the database name, not the SSH host
	cs, err := remotestorage.NewDoltChunkStoreFromPath(ctx, nbf, urlObj.Path, path, false, client)
	if err != nil {
		cmd.Process.Kill()
		grpcConn.Close()
		session.Close()
		return nil, nil, nil, fmt.Errorf("failed to create chunk store: %w", err)
	}
	
	// Store the command process in the context so it can be cleaned up later
	// The process needs to stay alive for the duration of the connection
	vrw := types.NewValueStore(cs)
	ns := tree.NewNodeStore(cs)
	db := datas.NewTypesDatabase(vrw, ns)
	
	return db, vrw, ns, nil
}

// bufferedPipeConn implements net.Conn over buffered stdin/stdout pipes
type bufferedPipeConn struct {
	stdin  io.WriteCloser
	stdout io.ReadCloser
	reader *bufio.Reader
	writer *bufio.Writer
}

func (c *bufferedPipeConn) Read(b []byte) (n int, err error) {
	return c.reader.Read(b)
}

func (c *bufferedPipeConn) Write(b []byte) (n int, err error) {
	n, err = c.writer.Write(b)
	if err != nil {
		return n, err
	}
	// Flush to ensure data is sent immediately
	return n, c.writer.Flush()
}

func (c *bufferedPipeConn) Close() error {
	// Flush any remaining buffered data
	c.writer.Flush()
	// Don't close the pipes here - they will be closed when the process exits
	return nil
}

// These are required for net.Conn but not used by gRPC
func (c *bufferedPipeConn) LocalAddr() net.Addr  { return &pipeAddr{} }
func (c *bufferedPipeConn) RemoteAddr() net.Addr { return &pipeAddr{} }
func (c *bufferedPipeConn) SetDeadline(t time.Time) error     { return nil }
func (c *bufferedPipeConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *bufferedPipeConn) SetWriteDeadline(t time.Time) error { return nil }

// pipeAddr implements net.Addr for pipe connections
type pipeAddr struct{}

func (a *pipeAddr) Network() string { return "pipe" }
func (a *pipeAddr) String() string  { return "pipe" }