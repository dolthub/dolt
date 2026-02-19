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
	"strings"
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

// SSHRemoteFactory creates databases backed by SSH remotes using the dolt
// transfer command for multiplexed gRPC+HTTP over stdin/stdout.
type SSHRemoteFactory struct{}

func (SSHRemoteFactory) PrepareDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) error {
	return fmt.Errorf("ssh scheme does not support PrepareDB")
}

// CreateDB creates a database backed by an SSH remote. It spawns a subprocess
// (either SSH or dolt transfer directly for localhost) and multiplexes gRPC
// and HTTP over the subprocess's stdin/stdout using SMUX.
func (SSHRemoteFactory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (datas.Database, types.ValueReadWriter, tree.NodeStore, error) {
	host := urlObj.Host
	path := urlObj.Path
	user := ""

	if urlObj.User != nil {
		user = urlObj.User.Username()
	}
	if atIdx := strings.LastIndex(host, "@"); atIdx != -1 {
		user = host[:atIdx]
		host = host[atIdx+1:]
	}

	cmd, err := buildTransferCommand(host, path, user)
	if err != nil {
		return nil, nil, nil, err
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	cmd.Stderr = io.Discard

	if err := cmd.Start(); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to start transfer subprocess: %w", err)
	}

	// Check for early exit (non-blocking).
	processDone := make(chan error, 1)
	go func() { processDone <- cmd.Wait() }()

	// Brief pause to detect immediate failures (e.g., bad path, missing dolt).
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-processDone:
		return nil, nil, nil, fmt.Errorf("transfer subprocess exited immediately: %w", err)
	default:
	}

	// Create SMUX client session over the subprocess pipes.
	pConn := &pipeConn{
		r:     stdout,
		w:     stdin,
		cmd:   cmd,
		stdin: stdin,
	}
	smuxConfig := smux.DefaultConfig()
	smuxConfig.MaxReceiveBuffer = 128 * 1024 * 1024
	smuxConfig.MaxStreamBuffer = 128 * 1024 * 1024

	session, err := smux.Client(pConn, smuxConfig)
	if err != nil {
		cmd.Process.Kill()
		return nil, nil, nil, fmt.Errorf("failed to create SMUX client session: %w", err)
	}

	// Register custom HTTP transport for the transfer host so table file
	// requests are routed through the SMUX session.
	transport := &smuxHTTPTransport{session: session}
	remotestorage.RegisterCustomTransport("transfer.local", transport)

	// Create gRPC client connection through SMUX streams.
	grpcConn, err := grpc.NewClient(
		"passthrough:///stdio",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, target string) (net.Conn, error) {
			return session.OpenStream()
		}),
		grpc.WithDisableHealthCheck(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		session.Close()
		cmd.Process.Kill()
		return nil, nil, nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}

	// Create chunk store backed by the remote gRPC service.
	client := remotesapi.NewChunkStoreServiceClient(grpcConn)
	cs, err := remotestorage.NewDoltChunkStoreFromPath(ctx, nbf, urlObj.Path, path, false, client)
	if err != nil {
		grpcConn.Close()
		session.Close()
		cmd.Process.Kill()
		return nil, nil, nil, fmt.Errorf("failed to create chunk store: %w", err)
	}

	// Wrap the chunk store with cleanup so resources are released when the
	// database is closed.
	conn := &sshConnection{
		cmd:      cmd,
		session:  session,
		grpcConn: grpcConn,
		stdin:    stdin,
	}
	wrappedCS := &sshChunkStore{DoltChunkStore: cs, conn: conn}

	vrw := types.NewValueStore(wrappedCS)
	ns := tree.NewNodeStore(wrappedCS)
	db := datas.NewTypesDatabase(vrw, ns)

	return db, vrw, ns, nil
}

// buildTransferCommand constructs the exec.Cmd for the transfer subprocess.
// For localhost without DOLT_SSH, it runs dolt transfer directly.
// For remote hosts, it runs ssh [user@]host "dolt --data-dir <path> transfer".
func buildTransferCommand(host, path, user string) (*exec.Cmd, error) {
	sshCommand := os.Getenv("DOLT_SSH")

	if host == "localhost" && sshCommand == "" {
		// Local testing mode: run dolt transfer directly.
		doltPath, err := os.Executable()
		if err != nil {
			doltPath = "dolt"
		}
		return exec.Command(doltPath, "--data-dir", path, "transfer"), nil
	}

	// Real SSH mode.
	if sshCommand == "" {
		sshCommand = "ssh"
	}

	sshTarget := host
	if user != "" {
		sshTarget = user + "@" + host
	}

	remoteCmd := fmt.Sprintf("dolt --data-dir %s transfer", path)
	sshArgs := strings.Fields(sshCommand)
	if len(sshArgs) == 0 {
		return nil, fmt.Errorf("invalid DOLT_SSH command: empty")
	}

	args := append(sshArgs[1:], sshTarget, remoteCmd)
	return exec.Command(sshArgs[0], args...), nil
}

// --- sshConnection: lifecycle management ---

// sshConnection holds all resources for an SSH transfer connection and
// implements coordinated cleanup.
type sshConnection struct {
	cmd      *exec.Cmd
	session  *smux.Session
	grpcConn *grpc.ClientConn
	stdin    io.WriteCloser
}

// Close releases all resources: unregisters the custom transport, closes
// the SMUX session, gRPC connection, and kills the subprocess.
func (c *sshConnection) Close() error {
	remotestorage.UnregisterCustomTransport("transfer.local")

	if c.session != nil {
		c.session.Close()
	}
	if c.grpcConn != nil {
		c.grpcConn.Close()
	}
	if c.stdin != nil {
		c.stdin.Close()
	}
	if c.cmd != nil && c.cmd.Process != nil {
		c.cmd.Process.Kill()
		c.cmd.Wait()
	}
	return nil
}

// --- sshChunkStore: wraps DoltChunkStore with cleanup ---

// sshChunkStore wraps a DoltChunkStore and closes the SSH connection when
// the chunk store is closed.
type sshChunkStore struct {
	*remotestorage.DoltChunkStore
	conn *sshConnection
}

func (s *sshChunkStore) Close() error {
	err := s.DoltChunkStore.Close()
	connErr := s.conn.Close()
	if err != nil {
		return err
	}
	return connErr
}

// --- smuxHTTPTransport: http.RoundTripper over SMUX ---

// smuxHTTPTransport implements http.RoundTripper by sending HTTP requests
// over SMUX streams. Each request gets its own stream.
type smuxHTTPTransport struct {
	session *smux.Session
}

func (t *smuxHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	stream, err := t.session.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("failed to open SMUX stream for HTTP: %w", err)
	}

	if err := req.Write(stream); err != nil {
		stream.Close()
		return nil, fmt.Errorf("failed to write HTTP request: %w", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(stream), req)
	if err != nil {
		stream.Close()
		return nil, fmt.Errorf("failed to read HTTP response: %w", err)
	}

	// Wrap the response body to close the SMUX stream when the body is closed.
	resp.Body = &streamBodyCloser{ReadCloser: resp.Body, stream: stream}
	return resp, nil
}

// streamBodyCloser wraps a response body and closes the underlying SMUX
// stream when the body is closed.
type streamBodyCloser struct {
	io.ReadCloser
	stream net.Conn
}

func (s *streamBodyCloser) Close() error {
	err := s.ReadCloser.Close()
	s.stream.Close()
	return err
}

// --- pipeConn: net.Conn over subprocess pipes ---

// pipeConn implements net.Conn over a subprocess's stdin/stdout pipes.
type pipeConn struct {
	r     io.ReadCloser
	w     io.WriteCloser
	cmd   *exec.Cmd       // prevents GC of subprocess
	stdin io.WriteCloser  // prevents GC of stdin pipe
}

func (c *pipeConn) Read(p []byte) (int, error)         { return c.r.Read(p) }
func (c *pipeConn) Write(p []byte) (int, error)        { return c.w.Write(p) }
func (c *pipeConn) Close() error                       { return nil }
func (c *pipeConn) LocalAddr() net.Addr                { return pipeAddr{} }
func (c *pipeConn) RemoteAddr() net.Addr               { return pipeAddr{} }
func (c *pipeConn) SetDeadline(_ time.Time) error      { return nil }
func (c *pipeConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *pipeConn) SetWriteDeadline(_ time.Time) error { return nil }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }
