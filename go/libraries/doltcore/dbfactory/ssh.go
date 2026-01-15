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
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"syscall"
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

// smuxTransport implements http.RoundTripper over SMUX streams
type smuxTransport struct {
	session *smux.Session
}

// RoundTrip sends an HTTP request over a new SMUX stream
func (t *smuxTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Open a new SMUX stream for this HTTP request
	stream, err := t.session.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("failed to open SMUX stream for HTTP: %w", err)
	}
	// Don't close the stream here - it needs to stay open for the response body to be read
	
	// Log the request headers for debugging
	f, _ := os.OpenFile("/tmp/ssh_factory.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f != nil {
		f.WriteString(fmt.Sprintf("HTTP request: %s %s, Range: %s\n", req.Method, req.URL.Path, req.Header.Get("Range")))
		f.Close()
	}
	
	// Write the HTTP request directly to the stream
	// The stream is already connected to the HTTP server
	err = req.Write(stream)
	if err != nil {
		stream.Close()
		return nil, fmt.Errorf("failed to write HTTP request: %w", err)
	}
	
	// Read the HTTP response from the stream
	// Use the stream directly, no buffering that could cause data loss
	resp, err := http.ReadResponse(bufio.NewReader(stream), req)
	if err != nil {
		stream.Close()
		return nil, fmt.Errorf("failed to read HTTP response: %w", err)
	}
	
	// Wrap the response body to close the stream when the body is closed
	// The response body already reads from the correct source
	resp.Body = &streamCloser{
		ReadCloser: resp.Body,
		stream:     stream,
		path:       req.URL.Path,
	}
	
	// Log for debugging
	f2, _ := os.OpenFile("/tmp/ssh_factory.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f2 != nil {
		f2.WriteString(fmt.Sprintf("HTTP response from %s: status %d, Content-Length: %s\n", 
			req.URL.Path, resp.StatusCode, resp.Header.Get("Content-Length")))
		f2.Close()
	}
	
	return resp, nil
}

// streamCloser wraps a response body and closes the underlying stream when closed  
type streamCloser struct {
	io.ReadCloser
	stream net.Conn
	path   string
}

func (sc *streamCloser) Read(p []byte) (n int, err error) {
	n, err = sc.ReadCloser.Read(p)
	// Log reads for debugging
	f, _ := os.OpenFile("/tmp/ssh_reads.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f != nil {
		f.WriteString(fmt.Sprintf("Read %d bytes from %s, err=%v, bufsize=%d\n", n, sc.path, err, len(p)))
		f.Close()
	}
	return n, err
}

func (sc *streamCloser) Close() error {
	err := sc.ReadCloser.Close()
	sc.stream.Close()
	return err
}

// Global variables to keep pipes alive across function returns
// This is a hack to prevent the pipes from being closed by GC
var (
	globalStdin io.WriteCloser
	globalStdout io.ReadCloser
	globalCmd *exec.Cmd
)

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
	// Use exec.Command instead of CommandContext to avoid the subprocess being killed when context is cancelled
	cmd := exec.Command(doltPath, "transfer", path)
	// Tell the transfer command to skip IO redirection
	cmd.Env = append(os.Environ(), "DOLT_SKIP_IO_REDIRECT=1")
	
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create stdin pipe: %w", err)
	}
	globalStdin = stdin  // Keep global reference
	
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	globalStdout = stdout  // Keep global reference
	globalCmd = cmd  // Keep global reference
	
	// Capture stderr to a file for debugging
	stderrFile, err := os.Create("/tmp/transfer_stderr.log")
	if err == nil {
		cmd.Stderr = stderrFile
		defer stderrFile.Close()
	} else {
		cmd.Stderr = os.Stderr
	}
	
	if err := cmd.Start(); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to start dolt transfer: %w", err)
	}
	
	// Monitor process in background but don't wait for it
	// The process should stay alive as long as we need it
	processDone := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		// Log when process exits
		f, _ := os.OpenFile("/tmp/process_exit.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString(fmt.Sprintf("Transfer process exited at %v with error: %v\n", time.Now(), err))
			f.Close()
		}
		processDone <- err
	}()
	
	// Give the server a moment to start
	time.Sleep(500 * time.Millisecond)
	
	// Check if process exited early (non-blocking)
	select {
	case err := <-processDone:
		// Process exited early, this is a problem
		return nil, nil, nil, fmt.Errorf("dolt transfer exited early: %w", err)
	default:
		// Process still running, continue
	}
	
	
	// Create SMUX client session over stdio
	stdioConn := &pipeConn{r: stdout, w: stdin, cmd: cmd, stdin: stdin, stdout: stdout}
	smuxConfig := smux.DefaultConfig()
	smuxConfig.MaxReceiveBuffer = 128 * 1024 * 1024
	smuxConfig.MaxStreamBuffer = 128 * 1024 * 1024
	
	session, err := smux.Client(stdioConn, smuxConfig)
	if err != nil {
		cmd.Process.Kill()
		return nil, nil, nil, fmt.Errorf("failed to create SMUX client session: %w", err)
	}
	
	// Register custom transport for transfer.local to route HTTP requests through SMUX
	smuxTransport := &smuxTransport{session: session}
	remotestorage.RegisterCustomTransport("transfer.local", smuxTransport)
	
	// Note: We should unregister this transport when the connection is closed,
	// but we don't have a good hook for that currently. The transport will
	// remain registered until the process exits.
	
	// Create gRPC client connection through SMUX
	grpcConn, err := grpc.NewClient(
		"passthrough:///stdio",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, target string) (net.Conn, error) {
			// Open a new SMUX stream for gRPC
			stream, err := session.OpenStream()
			if err != nil {
				return nil, fmt.Errorf("failed to open SMUX stream for gRPC: %w", err)
			}
			f, _ := os.OpenFile("/tmp/ssh_factory.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if f != nil {
				f.WriteString("Opened gRPC stream\n")
				f.Close()
			}
			return stream, nil
		}),
		grpc.WithDisableHealthCheck(),
		grpc.WithDefaultCallOptions(
			grpc.WaitForReady(true),
		),
	)
	if err != nil {
		cmd.Process.Kill()
		return nil, nil, nil, fmt.Errorf("failed to create grpc client: %w", err)
	}
	
	// Create chunk store client
	client := remotesapi.NewChunkStoreServiceClient(grpcConn)
	// Use the path as the database name, not the SSH host
	f, _ := os.OpenFile("/tmp/ssh_factory.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f != nil {
		f.WriteString(fmt.Sprintf("Creating chunk store with path: %s\n", urlObj.Path))
		f.Close()
	}
	cs, err := remotestorage.NewDoltChunkStoreFromPath(ctx, nbf, urlObj.Path, path, false, client)
	if err != nil {
		f, _ := os.OpenFile("/tmp/ssh_factory.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString(fmt.Sprintf("Failed to create chunk store, killing process: %v\n", err))
			f.Close()
		}
		cmd.Process.Kill()
		grpcConn.Close()
		return nil, nil, nil, fmt.Errorf("failed to create chunk store: %w", err)
	}
	f, _ = os.OpenFile("/tmp/ssh_factory.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f != nil {
		f.WriteString("Successfully created chunk store\n")
		f.Close()
	}
	
	// Store the command process in the context so it can be cleaned up later
	// The process needs to stay alive for the duration of the connection
	vrw := types.NewValueStore(cs)
	ns := tree.NewNodeStore(cs)
	db := datas.NewTypesDatabase(vrw, ns)
	
	f, _ = os.OpenFile("/tmp/ssh_factory.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f != nil {
		f.WriteString("About to return from NewRemoteDB, sleeping 2s first\n")
		f.Close()
	}
	
	// Sleep before returning to see if it's a timing issue
	time.Sleep(2 * time.Second)
	
	// Keep the subprocess and session alive by monitoring in the background
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			if cmd.Process == nil {
				break
			}
			// Check if the process is still running
			if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
				f, _ := os.OpenFile("/tmp/ssh_factory.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
				if f != nil {
					f.WriteString(fmt.Sprintf("Subprocess died: %v\n", err))
					f.Close()
				}
				// Close the session if process dies
				session.Close()
				break
			}
		}
	}()
	
	return db, vrw, ns, nil
}

// pipeConn implements net.Conn over exec.Command pipes
type pipeConn struct {
	r io.ReadCloser
	w io.WriteCloser
	cmd *exec.Cmd  // Keep a reference to prevent GC
	stdin io.WriteCloser  // Keep stdin alive
	stdout io.ReadCloser  // Keep stdout alive
}

func (c *pipeConn) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

func (c *pipeConn) Write(p []byte) (int, error) {
	return c.w.Write(p)
}

func (c *pipeConn) Close() error {
	// Log when Close is called
	f, _ := os.OpenFile("/tmp/pipe_close.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f != nil {
		f.WriteString(fmt.Sprintf("pipeConn.Close() called at %v\n", time.Now()))
		f.Close()
	}
	// Don't actually close the pipes - they need to stay open
	// for the duration of the connection
	return nil
}

func (c *pipeConn) LocalAddr() net.Addr                { return &pipeAddr{} }
func (c *pipeConn) RemoteAddr() net.Addr               { return &pipeAddr{} }
func (c *pipeConn) SetDeadline(t time.Time) error      { return nil }
func (c *pipeConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *pipeConn) SetWriteDeadline(t time.Time) error { return nil }

// pipeAddr implements net.Addr for pipe connections
type pipeAddr struct{}

func (a *pipeAddr) Network() string { return "pipe" }
func (a *pipeAddr) String() string  { return "pipe" }


