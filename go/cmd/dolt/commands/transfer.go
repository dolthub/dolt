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
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/xtaci/smux"
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

This is a low-level command not intended for direct use.`,
	Synopsis: []string{
		"<path>",
	},
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

	ddb := dEnv.DoltDB(ctx)

	// Database should already be loaded by the caller. Being very safe since there are some late binding checks.
	if ddb == nil || dEnv.DBLoadError != nil {
		if dEnv.DBLoadError != nil {
			return HandleVErrAndExitCode(errhand.BuildDError("failed to load database").AddCause(dEnv.DBLoadError).Build(), usage)
		}
		return HandleVErrAndExitCode(errhand.BuildDError("failed to load database").Build(), usage)
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

	// Set up gRPC chunk store service backed by this database.
	db := doltdb.ExposeDatabaseFromDoltDB(ddb)
	cs := datas.ChunkStoreFromDatabase(db)

	// GenerationalChunkStore implements RemoteSrvStore, so this is going to work for any "normal" Dole db.
	if _, ok := cs.(remotesrv.RemoteSrvStore); !ok {
		return HandleVErrAndExitCode(errhand.BuildDError("chunk store does not implement RemoteSrvStore").Build(), usage)
	}
	dbCache := &singletonDBCache{cs: cs.(remotesrv.RemoteSrvStore)}

	logger := logrus.New()
	logger.SetOutput(os.Stderr)
	logEntry := logrus.NewEntry(logger)

	sealer := &identitySealer{}
	chunkStoreService := remotesrv.NewHttpFSBackedChunkStore(
		logEntry,
		transferHost,
		dbCache,
		dEnv.FS,
		"http",
		remotesapi.PushConcurrencyControl_PUSH_CONCURRENCY_CONTROL_UNSPECIFIED,
		sealer,
	)

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(remotesrv.MaxGRPCMessageSize),
		grpc.MaxSendMsgSize(remotesrv.MaxGRPCMessageSize),
	)
	remotesapi.RegisterChunkStoreServiceServer(grpcServer, chunkStoreService)

	// Set up HTTP handler for table file transfers.
	httpHandler := newTransferFileHandler(dbCache, dEnv.FS, logEntry)
	httpServer := &http.Server{Handler: httpHandler}

	// Create SMUX-backed listeners for gRPC and HTTP.
	listener := &smuxListener{session: session}

	// Start both servers.
	errCh := make(chan error, 2)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			errCh <- fmt.Errorf("gRPC server error: %w", err)
		}
	}()
	go func() {
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("HTTP server error: %w", err)
		}
	}()

	// Wait for session close, server error, or context cancellation.
	select {
	case err := <-errCh:
		// We get away with printing directly to stderr here since transfer command is special-cased to leave IO streams alone.
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	case <-session.CloseChan():
		return 0
	case <-ctx.Done():
		return 0
	}
}

// transferHost is the virtual hostname used for HTTP requests routed through
// the SMUX transport. The client registers a custom HTTP transport for this
// host so requests are routed through the SSH connection rather than the network.
const transferHost = "transfer.local"

// identitySealer is a no-op Sealer for the local stdio transport where URL
// sealing/unsealing is unnecessary.
type identitySealer struct{}

func (identitySealer) Seal(u *url.URL) (*url.URL, error)   { return u, nil }
func (identitySealer) Unseal(u *url.URL) (*url.URL, error) { return u, nil }

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

// transferFileHandler serves table files over HTTP through the SMUX transport.
type transferFileHandler struct {
	dbCache remotesrv.DBCache
	fs      filesys.Filesys
	lgr     *logrus.Entry
}

func newTransferFileHandler(dbCache remotesrv.DBCache, fs filesys.Filesys, lgr *logrus.Entry) *transferFileHandler {
	return &transferFileHandler{
		dbCache: dbCache,
		fs:      fs,
		lgr:     lgr,
	}
}

func (fh *transferFileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimLeft(r.URL.Path, "/")

	switch r.Method {
	case http.MethodGet:
		fh.handleGet(w, r, path)
	case http.MethodPost, http.MethodPut:
		fh.handleUpload(w, r, path)
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func (fh *transferFileHandler) handleGet(w http.ResponseWriter, r *http.Request, path string) {
	reader, err := fh.fs.OpenForRead(path)
	if err != nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	defer reader.Close()

	// The underlying reader is an *os.File which implements io.ReadSeeker.
	// http.ServeContent handles full and range requests with streaming.
	rs, ok := reader.(io.ReadSeeker)
	if !ok {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, "", time.Time{}, rs)
}

func (fh *transferFileHandler) handleUpload(w http.ResponseWriter, r *http.Request, path string) {
	i := strings.LastIndex(path, "/")
	if i < 0 {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	dbPath := path[:i]
	filename := path[i+1:]
	q := r.URL.Query()

	numChunksStr := q.Get("num_chunks")
	if numChunksStr == "" {
		http.Error(w, "Bad Request: num_chunks required", http.StatusBadRequest)
		return
	}
	numChunks, err := strconv.Atoi(numChunksStr)
	if err != nil {
		http.Error(w, "Bad Request: invalid num_chunks", http.StatusBadRequest)
		return
	}

	contentLengthStr := q.Get("content_length")
	if contentLengthStr == "" {
		http.Error(w, "Bad Request: content_length required", http.StatusBadRequest)
		return
	}
	contentLength, err := strconv.ParseUint(contentLengthStr, 10, 64)
	if err != nil {
		http.Error(w, "Bad Request: invalid content_length", http.StatusBadRequest)
		return
	}

	contentHashStr := q.Get("content_hash")
	if contentHashStr == "" {
		http.Error(w, "Bad Request: content_hash required", http.StatusBadRequest)
		return
	}
	contentHash, err := base64.RawURLEncoding.DecodeString(contentHashStr)
	if err != nil {
		http.Error(w, "Bad Request: invalid content_hash", http.StatusBadRequest)
		return
	}

	splitOffset := uint64(0)
	if splitOffsetStr := q.Get("split_offset"); splitOffsetStr != "" {
		splitOffset, err = strconv.ParseUint(splitOffsetStr, 10, 64)
		if err != nil {
			http.Error(w, "Bad Request: invalid split_offset", http.StatusBadRequest)
			return
		}
	}

	cs, err := fh.dbCache.Get(r.Context(), dbPath, "")
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	err = cs.WriteTableFile(r.Context(), filename, splitOffset, numChunks, contentHash, func() (io.ReadCloser, uint64, error) {
		return r.Body, contentLength, nil
	})
	if err != nil {
		fh.lgr.WithError(err).Error("failed to write table file")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
