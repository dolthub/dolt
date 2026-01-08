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

package commands

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
	"google.golang.org/grpc"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
)

type TransferCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd TransferCmd) Name() string {
	return "transfer"
}

// Description returns a description of the command
func (cmd TransferCmd) Description() string {
	return "Transfer data to/from remote over stdin/stdout"
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd TransferCmd) RequiresRepo() bool {
	return false
}

// Hidden should return true if this command should be hidden from the help text
func (cmd TransferCmd) Hidden() bool {
	return true
}

func (cmd TransferCmd) Docs() *cli.CommandDocumentation {
	return nil
}

func (cmd TransferCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs(cmd.Name())
	return ap
}

// Exec executes the command
func (cmd TransferCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	// Debug logging to file
	ioutil.WriteFile("/tmp/transfer_start.log", []byte(fmt.Sprintf("Transfer started with args: %v\n", args)), 0644)

	ap := cmd.ArgParser()
	apr, err := ap.Parse(args)
	if err != nil {
		ioutil.WriteFile("/tmp/transfer_error.log", []byte(fmt.Sprintf("Parse error: %v\n", err)), 0644)
		return 1
	}

	// Get the repository path from arguments and change to that directory
	if len(apr.Args) > 0 {
		repoPath := apr.Args[0]
		ioutil.WriteFile("/tmp/transfer_path.log", []byte(fmt.Sprintf("Changing to: %s\n", repoPath)), 0644)
		// Change to the repository directory
		if err := os.Chdir(repoPath); err != nil {
			ioutil.WriteFile("/tmp/transfer_error.log", []byte(fmt.Sprintf("Chdir error: %v\n", err)), 0644)
			return 1
		}
		// Create a new filesystem for the new directory
		fs, err := filesys.LocalFilesysWithWorkingDir(".")
		if err != nil {
			ioutil.WriteFile("/tmp/transfer_error.log", []byte(fmt.Sprintf("Filesys error: %v\n", err)), 0644)
			return 1
		}
		// Reload environment in the new directory
		dEnv = env.Load(ctx, env.GetCurrentUserHomeDir, fs, doltdb.LocalDirDoltDB, "dolt")
	}

	// Load the database
	ddb := dEnv.DoltDB(ctx)
	if ddb == nil || dEnv.DBLoadError != nil {
		// Write error to a debug file since we can't use stderr
		if dEnv.DBLoadError != nil {
			ioutil.WriteFile("/tmp/transfer_error.log", []byte(fmt.Sprintf("DBLoadError: %v\n", dEnv.DBLoadError)), 0644)
		} else {
			ioutil.WriteFile("/tmp/transfer_error.log", []byte("DoltDB is nil\n"), 0644)
		}
		return 1
	}

	// Create a gRPC server for the remote API
	// Use larger buffer sizes to handle chunked data
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(128*1024*1024),
		grpc.MaxSendMsgSize(128*1024*1024),
		grpc.InitialWindowSize(1<<20),
		grpc.InitialConnWindowSize(1<<20),
	)

	// Get the chunk store from the database
	db := doltdb.HackDatasDatabaseFromDoltDB(ddb)
	cs := datas.ChunkStoreFromDatabase(db)
	dbCache := &singletonDBCache{cs: cs.(remotesrv.RemoteSrvStore)}

	// Create and register the chunk store service
	// Disable logging to avoid interfering with stdio communication
	logger := logrus.New()
	logger.SetOutput(ioutil.Discard)
	logEntry := logrus.NewEntry(logger)

	// Create a wrapper service that logs calls
	origService := remotesrv.NewHttpFSBackedChunkStore(logEntry, "", dbCache, dEnv.FS, "stdio", remotesapi.PushConcurrencyControl_PUSH_CONCURRENCY_CONTROL_UNSPECIFIED, nil)
	chunkStoreService := &debugChunkStoreService{ChunkStoreServiceServer: origService}

	remotesapi.RegisterChunkStoreServiceServer(grpcServer, chunkStoreService)

	// Create a buffered stdio connection to prevent deadlocks
	stdioConn := &bufferedStdioConn{
		stdin:  os.Stdin,
		stdout: os.Stdout,
		reader: bufio.NewReader(os.Stdin),
		writer: bufio.NewWriter(os.Stdout),
	}

	// Create SMUX server over stdio with default config
	ioutil.WriteFile("/tmp/transfer_smux.log", []byte("Creating SMUX server\n"), 0644)
	session, err := smux.Server(stdioConn, nil)
	if err != nil {
		ioutil.WriteFile("/tmp/transfer_error.log", []byte(fmt.Sprintf("SMUX error: %v\n", err)), 0644)
		return 1
	}
	defer session.Close()

	// Create a listener that accepts SMUX streams as connections
	listener := &smuxListener{
		session: session,
		closed:  make(chan struct{}),
	}

	// Serve gRPC over the SMUX session
	// This will block until the listener is closed or an error occurs
	ioutil.WriteFile("/tmp/transfer_serve.log", []byte("Starting gRPC server\n"), 0644)
	err = grpcServer.Serve(listener)
	if err != nil && err != grpc.ErrServerStopped {
		ioutil.WriteFile("/tmp/transfer_serve_error.log", []byte(fmt.Sprintf("Serve error: %v\n", err)), 0644)
		return 1
	}

	return 0
}

// smuxListener implements net.Listener for SMUX sessions
type smuxListener struct {
	session *smux.Session
	closed  chan struct{}
}

func (l *smuxListener) Accept() (net.Conn, error) {
	stream, err := l.session.AcceptStream()
	if err != nil {
		ioutil.WriteFile("/tmp/transfer_accept_error.log", []byte(fmt.Sprintf("Accept error: %v\n", err)), 0644)
		return nil, err
	}
	ioutil.WriteFile("/tmp/transfer_accept.log", []byte("Accepted SMUX stream\n"), 0644)
	return stream, nil
}

func (l *smuxListener) Close() error {
	close(l.closed)
	return l.session.Close()
}

func (l *smuxListener) Addr() net.Addr {
	return &stdioAddr{}
}

// singletonDBCache implements remotesrv.DBCache for a single database
type singletonDBCache struct {
	cs remotesrv.RemoteSrvStore
}

func (s *singletonDBCache) Get(ctx context.Context, path, nbfVerStr string) (remotesrv.RemoteSrvStore, error) {
	return s.cs, nil
}

// bufferedStdioConn implements net.Conn over buffered stdin/stdout
type bufferedStdioConn struct {
	stdin  io.Reader
	stdout io.Writer
	reader *bufio.Reader
	writer *bufio.Writer
}

func (c *bufferedStdioConn) Read(b []byte) (n int, err error) {
	return c.reader.Read(b)
}

func (c *bufferedStdioConn) Write(b []byte) (n int, err error) {
	n, err = c.writer.Write(b)
	if err != nil {
		return n, err
	}
	// Flush to ensure data is sent immediately
	return n, c.writer.Flush()
}

func (c *bufferedStdioConn) Close() error {
	// Flush any remaining buffered data
	c.writer.Flush()
	// Don't actually close stdin/stdout - they belong to the process
	return nil
}

// stdioAddr implements net.Addr for stdio connections
type stdioAddr struct{}

func (a *stdioAddr) Network() string { return "stdio" }
func (a *stdioAddr) String() string  { return "stdio" }

// debugChunkStoreService wraps the real service to log calls
type debugChunkStoreService struct {
	remotesapi.ChunkStoreServiceServer
}

func (d *debugChunkStoreService) GetRepoMetadata(ctx context.Context, req *remotesapi.GetRepoMetadataRequest) (*remotesapi.GetRepoMetadataResponse, error) {
	ioutil.WriteFile("/tmp/transfer_metadata.log", []byte("GetRepoMetadata called\n"), 0644)
	return d.ChunkStoreServiceServer.GetRepoMetadata(ctx, req)
}

// These are required for net.Conn but not used by gRPC
func (c *bufferedStdioConn) LocalAddr() net.Addr                { return &stdioAddr{} }
func (c *bufferedStdioConn) RemoteAddr() net.Addr               { return &stdioAddr{} }
func (c *bufferedStdioConn) SetDeadline(t time.Time) error      { return nil }
func (c *bufferedStdioConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *bufferedStdioConn) SetWriteDeadline(t time.Time) error { return nil }
