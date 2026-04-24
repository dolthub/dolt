// Copyright 2026 Dolthub, Inc.
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
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

// TransferCmd serves repository data over stdin/stdout for SSH remote operations.
type TransferCmd struct{}

func (cmd TransferCmd) Name() string {
	return "transfer"
}

func (cmd TransferCmd) Description() string {
	return "Transfer data to/from remote over stdin/stdout"
}

func (cmd TransferCmd) RequiresRepo() bool {
	return false
}

func (cmd TransferCmd) Hidden() bool {
	return true
}

func (cmd TransferCmd) InstallsSignalHandlers() bool {
	return true
}

var transferDocs = cli.CommandDocumentationContent{
	ShortDesc: "Internal command for SSH remote operations",
	LongDesc: `The transfer command is used internally by Dolt for SSH remote operations.
It serves repository data over stdin/stdout using multiplexed gRPC and HTTP protocols.

This command is typically invoked by SSH when cloning or pushing to SSH remotes:
  ssh user@host "dolt --data-dir /path/to/repo transfer"

The transfer command:
  - Loads the Dolt database at the specified path
  - Starts a gRPC server for chunk store operations
  - Starts an HTTP server for table file transfers
  - Multiplexes both protocols over stdin/stdout using SMUX

The exit code of the transfer command is not meaningful to callers. All errors
are surfaced through the gRPC and HTTP responses on the multiplexed IO streams.
The client detects a failed subprocess via pipe EOF, which closes the SMUX
session and cancels in-flight operations.

This is a low-level command not intended for direct use.`,
}

func (cmd TransferCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(transferDocs, ap)
}

func (cmd TransferCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs(cmd.Name())
	return ap
}

func (cmd TransferCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, transferDocs, ap))
	_ = cli.ParseArgsOrDie(ap, args, help)

	// Ignore SIGPIPE to prevent broken pipe crashes during SSH disconnect.
	signal.Ignore(syscall.SIGPIPE)

	cs, serverFS, err := resolveDatabaseChunkStore(ctx, dEnv)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	// Create SMUX session (server mode) over stdin/stdout.
	conn := newStdioConn(os.Stdin, os.Stdout)
	smuxConfig := smux.DefaultConfig()
	smuxConfig.MaxReceiveBuffer = remotesrv.MaxGRPCMessageSize
	smuxConfig.MaxStreamBuffer = remotesrv.MaxGRPCMessageSize

	session, err := smux.Server(conn, smuxConfig)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("failed to create SMUX session").AddCause(err).Build(), usage)
	}
	defer session.Close()

	dbCache := &singletonDBCache{cs: cs}

	logger := logrus.New()
	logger.SetOutput(os.Stderr)
	logEntry := logrus.NewEntry(logger)

	sealer := &passThruSealer{}
	chunkStoreService := remotesrv.NewHttpFSBackedChunkStore(
		logEntry,
		transferHost,
		dbCache,
		serverFS,
		"http",
		remotesapi.PushConcurrencyControl_PUSH_CONCURRENCY_CONTROL_UNSPECIFIED,
		sealer,
		nil,
	)

	// gRPC for chunk store operations.
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(remotesrv.MaxGRPCMessageSize),
		grpc.MaxSendMsgSize(remotesrv.MaxGRPCMessageSize),
	)
	remotesapi.RegisterChunkStoreServiceServer(grpcServer, chunkStoreService)

	// Http handler for storage file transfers -- reuse remotesrv's filehandler.
	fileServer := remotesrv.NewFileHandler(logEntry, dbCache, serverFS, false, sealer, true)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
		} else {
			fileServer.ServeHTTP(w, r)
		}
	})
	h2s := &http2.Server{}
	httpServer := &http.Server{Handler: h2c.NewHandler(handler, h2s)}

	listener := &smuxListener{session: session}

	errCh := make(chan error, 1)
	go func() {
		if err := httpServer.Serve(listener); err != nil {
			errCh <- fmt.Errorf("server error: %w", err)
		}
	}()

	// Wait for session close, server error, or context cancellation.
	select {
	case err := <-errCh:
		// We get away with printing directly to stderr here since transfer command is special-cased to leave IO streams alone.
		fmt.Fprintf(os.Stderr, "%v\n", err)
		// Transfer command exit code is not meaningful to callers. HTTP/gRPC Errors rule.
		return 0
	case <-session.CloseChan():
		return 0
	case <-ctx.Done():
		return 0
	}
}

// resolveDatabaseChunkStore inspects the environment loaded from --data-dir and
// returns the RemoteSrvStore to serve and the filesystem root to use for HTTP
// file serving.
//
// Two on-disk formats are supported:
//
//  1. Normal dolt repository (.dolt/noms/ present): dEnv.DoltDB loads successfully.
//     The FS is already rooted at the repo directory and cs.Path() resolves to
//     .dolt/noms/ inside it, giving HTTP paths like ".dolt/noms/<hash>".
//
//  2. Bare repository (no .dolt/noms/): the --data-dir path is a flat NBS directory
//     (a NomsBlockStore). This is the format produced by pushing to an empty directory
//     or by the standalone remotesrv binary.
//
//     For bare repos the HTTP filesystem root is set to the *parent* of the bare
//     directory so that cs.Path() is a named subdirectory of the root. The gRPC
//     handler computes prefix = filepath.Rel(fsRoot, cs.Path()); with the parent
//     as root this yields "<dirName>", so HTTP paths are "<dirName>/<hash>". If
//     the root equalled cs.Path() the prefix would be "." and the file handler
//     would return HTTP 400 because it requires at least one "/" in the path.
func resolveDatabaseChunkStore(ctx context.Context, dEnv *env.DoltEnv) (remotesrv.RemoteSrvStore, filesys.Filesys, error) {
	ddb := dEnv.DoltDB(ctx)
	if ddb != nil && dEnv.DBLoadError == nil {
		// Normal dolt repository.
		db := doltdb.ExposeDatabaseFromDoltDB(ddb)
		rawCS := datas.ChunkStoreFromDatabase(db)
		rss, ok := rawCS.(remotesrv.RemoteSrvStore)
		if !ok {
			return nil, nil, fmt.Errorf("chunk store does not implement RemoteSrvStore")
		}
		return rss, dEnv.FS, nil
	}

	if !errors.Is(dEnv.DBLoadError, doltdb.ErrMissingDoltDataDir) {
		return nil, nil, fmt.Errorf("failed to load database: %w", dEnv.DBLoadError)
	}

	// Bare repository: a flat NBS directory with no .dolt/ structure.
	absPath, err := dEnv.FS.Abs(".")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to resolve bare repository path: %w", err)
	}
	q := nbs.NewUnlimitedMemQuotaProvider()
	cs, err := nbs.NewLocalStore(ctx, types.Format_Default.VersionString(), absPath, remotesrv.MaxGRPCMessageSize, q, false)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open bare repository at %s: %w", absPath, err)
	}

	// Root the HTTP filesystem at the parent so cs.Path() is a named subdirectory.
	parentPath := filepath.Dir(absPath)
	serverFS, err := filesys.LocalFilesysWithWorkingDir(parentPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to set up file server root at %s: %w", parentPath, err)
	}
	return cs, serverFS, nil
}

// transferHost is the virtual hostname used for HTTP requests routed through
// the SMUX transport. The client registers a custom HTTP transport for this
// host so requests are routed through the SSH connection rather than the network.
const transferHost = "transfer.local"

// passThruSealer is a no-op Sealer for the local stdio transport where URL
// sealing/unsealing is unnecessary.
type passThruSealer struct{}

func (passThruSealer) Seal(u *url.URL) (*url.URL, error)   { return u, nil }
func (passThruSealer) Unseal(u *url.URL) (*url.URL, error) { return u, nil }

// singletonDBCache implements remotesrv.DBCache for a single database,
// always returning the same chunk store regardless of path.
type singletonDBCache struct {
	cs remotesrv.RemoteSrvStore
}

func (sdbc *singletonDBCache) Get(_ context.Context, _, _ string) (remotesrv.RemoteSrvStore, error) {
	return sdbc.cs, nil
}

// stdioConn wraps an io.Reader and io.Writer as a net.Conn for use with SMUX.
type stdioConn struct {
	r io.Reader
	w io.Writer
}

func newStdioConn(r io.Reader, w io.Writer) *stdioConn {
	return &stdioConn{r: r, w: w}
}

func (c *stdioConn) Read(p []byte) (int, error)  { return c.r.Read(p) }
func (c *stdioConn) Write(p []byte) (int, error) { return c.w.Write(p) }
func (c *stdioConn) Close() error {
	// No-op. Closing the wrapped streams breaks the SSH connection, so we rely on the process terminating to clean up
	//resources. SMUX will detect EOF and close the session appropriately.
	return nil
}

func (c *stdioConn) LocalAddr() net.Addr                { return stdioAddr{} }
func (c *stdioConn) RemoteAddr() net.Addr               { return stdioAddr{} }
func (c *stdioConn) SetDeadline(_ time.Time) error      { return nil }
func (c *stdioConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *stdioConn) SetWriteDeadline(_ time.Time) error { return nil }

// stdioAddr is a net.Addr implementation for the stdioConn, returning fixed values since there is no real network address.
type stdioAddr struct{}

func (stdioAddr) Network() string { return "stdio" }
func (stdioAddr) String() string  { return "stdio" }

// smuxListener implements net.Listener by accepting SMUX streams from a session.
type smuxListener struct {
	session *smux.Session
}

func (l *smuxListener) Accept() (net.Conn, error) {
	return l.session.AcceptStream()
}

func (l *smuxListener) Close() error   { return nil }
func (l *smuxListener) Addr() net.Addr { return stdioAddr{} }
