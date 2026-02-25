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
	"bytes"
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
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
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
	host := urlObj.Hostname()
	port := urlObj.Port()
	path := urlObj.Path
	user := ""

	// Strip trailing /.dolt if provided.
	path = strings.TrimSuffix(path, "/.dolt")

	if urlObj.User != nil {
		user = urlObj.User.Username()
	}
	if atIdx := strings.LastIndex(host, "@"); atIdx != -1 {
		user = host[:atIdx]
		host = host[atIdx+1:]
	}

	cmd, err := buildTransferCommand(host, port, path, user)
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

	// Read stderr via a pipe so we control when it is fully consumed.
	// stderrDone channel is closed once all stderr has been read, sending signal that the subcommand has terminated.
	stderrDone := make(chan struct{})
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create stderr pipe: %w", err)
	}
	var stderrBuf bytes.Buffer
	go func() {
		io.Copy(io.MultiWriter(os.Stderr, &stderrBuf), stderrPipe)
		close(stderrDone)
	}()

	if err := cmd.Start(); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to start transfer subprocess: %w", err)
	}

	procCtx, procCancel := context.WithCancelCause(ctx)

	// Create SMUX client session over the subprocess pipes.
	pConn := &pipeConn{
		r: stdout,
		w: stdin,
	}
	smuxConfig := smux.DefaultConfig()
	smuxConfig.MaxReceiveBuffer = remotesrv.MaxGRPCMessageSize
	smuxConfig.MaxStreamBuffer = remotesrv.MaxGRPCMessageSize

	session, err := smux.Client(pConn, smuxConfig)
	if err != nil {
		cmd.Process.Kill()
		procCancel(err)
		return nil, nil, nil, sshRemoteError(stderrDone, &stderrBuf, path, "failed to create SMUX client session", err)
	}

	// Monitor the SMUX session in a background goroutine. When the remote
	// subprocess exits (bad path, missing dolt, SSH failure, etc.), the pipe
	// gets EOF and SMUX closes the session. This cancels our context so that
	// gRPC calls unblock immediately instead of hanging forever.
	//
	// AcceptStream forces SMUX to actively read the connection. Without it,
	// SMUX only discovers EOF on the next read/write attempt -- which never
	// comes while gRPC is stuck in the WaitForReady picker loop.
	go func() {
		session.AcceptStream()
		procCancel(fmt.Errorf("remote process exited"))
	}()

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
		procCancel(err)
		return nil, nil, nil, sshRemoteError(stderrDone, &stderrBuf, path, "failed to create gRPC client", err)
	}

	// Create chunk store backed by the remote gRPC service.
	client := remotesapi.NewChunkStoreServiceClient(grpcConn)
	cs, err := remotestorage.NewDoltChunkStoreFromPath(procCtx, nbf, urlObj.Path, path, false, client)
	if err != nil {
		procCancel(err)
		remotestorage.UnregisterCustomTransport("transfer.local")
		grpcConn.Close()
		session.Close()
		stdin.Close()
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		return nil, nil, nil, sshRemoteError(stderrDone, &stderrBuf, path, "failed to create chunk store", err)
	}

	// Wrap the chunk store with cleanup so resources are released when the
	// database is closed.
	conn := &sshConnection{
		cmd:        cmd,
		session:    session,
		grpcConn:   grpcConn,
		stdin:      stdin,
		procCancel: procCancel,
	}
	wrappedCS := &sshChunkStore{DoltChunkStore: cs, conn: conn}

	vrw := types.NewValueStore(wrappedCS)
	ns := tree.NewNodeStore(wrappedCS)
	db := datas.NewTypesDatabase(vrw, ns)

	return db, vrw, ns, nil
}

// sshRemoteError builds an error message for SSH remote failures. It waits
// for the remote's stderr to be fully read (signaled by stderrDone) and
// uses it to produce a more informative message than the raw gRPC/SMUX error.
func sshRemoteError(stderrDone <-chan struct{}, stderrBuf *bytes.Buffer, path, msg string, err error) error {
	<-stderrDone
	errMsg := filterSSHNoise(stderrBuf.String())
	if errMsg != "" {
		if strings.Contains(errMsg, "no such file or directory") || strings.Contains(errMsg, "failed to load database") {
			return fmt.Errorf("repository not found at %s", path)
		}
		return fmt.Errorf("%s: remote: %s", msg, errMsg)
	}
	return fmt.Errorf("%s: %w", msg, err)
}

// filterSSHNoise removes common SSH informational messages from stderr output
// so they are not mistaken for real errors.
func filterSSHNoise(s string) string {
	var lines []string
	for _, line := range strings.Split(s, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "Warning: Permanently added") {
			continue
		}
		lines = append(lines, trimmed)
	}
	return strings.Join(lines, "\n")
}

// buildTransferCommand constructs the exec.Cmd for the transfer subprocess.
// It runs ssh [-p port] [user@]host "<dolt> --data-dir <path> transfer",
// using DOLT_SSH as the SSH binary if set (default "ssh"), and
// DOLT_SSH_EXEC_PATH as the remote dolt binary path if set (default "dolt").
func buildTransferCommand(host, port, path, user string) (*exec.Cmd, error) {
	sshCommand := os.Getenv("DOLT_SSH")
	if sshCommand == "" {
		sshCommand = "ssh"
	}

	remoteDolt := os.Getenv("DOLT_SSH_EXEC_PATH")
	if remoteDolt == "" {
		remoteDolt = "dolt"
	}

	sshTarget := host
	if user != "" {
		sshTarget = user + "@" + host
	}

	remoteCmd := fmt.Sprintf("%s --data-dir %s transfer", remoteDolt, path)
	sshArgs := strings.Fields(sshCommand)
	if len(sshArgs) == 0 {
		return nil, fmt.Errorf("invalid DOLT_SSH command: empty")
	}

	args := append(sshArgs[1:], "-p", port, sshTarget, remoteCmd)
	if port == "" {
		args = append(sshArgs[1:], sshTarget, remoteCmd)
	}
	return exec.Command(sshArgs[0], args...), nil
}

// --- sshConnection: lifecycle management ---

// sshConnection holds all resources for an SSH transfer connection and
// implements coordinated cleanup.
type sshConnection struct {
	cmd        *exec.Cmd
	session    *smux.Session
	grpcConn   *grpc.ClientConn
	stdin      io.WriteCloser
	procCancel context.CancelCauseFunc
}

// Close releases all resources: unregisters the custom transport, closes
// the SMUX session, gRPC connection, and kills the subprocess.
func (c *sshConnection) Close() error {
	remotestorage.UnregisterCustomTransport("transfer.local")

	if c.procCancel != nil {
		c.procCancel(fmt.Errorf("connection closed"))
	}
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
	r io.ReadCloser
	w io.WriteCloser
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
