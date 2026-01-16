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
	"context"
	"crypto/md5"
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
	"sync"
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

// identitySealer is a no-op sealer for local stdio transport
type identitySealer struct{}

func (identitySealer) Seal(u *url.URL) (*url.URL, error) {
	return u, nil
}

func (identitySealer) Unseal(u *url.URL) (*url.URL, error) {
	return u, nil
}

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

// InstallsSignalHandlers tells the framework not to install signal handlers
func (cmd TransferCmd) InstallsSignalHandlers() bool {
	return true  // We don't want signal handlers interfering with stdio
}

var transferDocs = cli.CommandDocumentationContent{
	ShortDesc: "Internal command for SSH remote operations",
	LongDesc: `The transfer command is used internally by Dolt for SSH remote operations.
It serves repository data over stdin/stdout using multiplexed gRPC and HTTP protocols.

This command is typically invoked by SSH when cloning or pushing to SSH remotes:
  ssh user@host "dolt transfer /path/to/repo"

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

// Exec executes the command
func (cmd TransferCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	// Parse arguments first
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, transferDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	
	// After this point, we're actually running the transfer command
	// Ignore all signals to prevent being killed
	signal.Ignore(syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGPIPE)
	
	// Catch any panic and log it
	defer func() {
		if r := recover(); r != nil {
			f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if f != nil {
				f.WriteString(fmt.Sprintf("PANIC in transfer command: %v\n", r))
				f.Close()
			}
			panic(r) // Re-panic after logging
		}
	}()
	
	// Log the start
	f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f != nil {
		f.WriteString(fmt.Sprintf("Transfer starting with args: %v, env DOLT_SKIP_IO_REDIRECT=%s\n", args, os.Getenv("DOLT_SKIP_IO_REDIRECT")))
		f.Close()
	}

	// Get the repository path from arguments and change to that directory
	if len(apr.Args) == 0 {
		return HandleVErrAndExitCode(errhand.BuildDError("repository path required").SetPrintUsage().Build(), usage)
	}
	
	repoPath := apr.Args[0]
	f2, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f2 != nil {
		f2.WriteString(fmt.Sprintf("Changing to directory: %s\n", repoPath))
		f2.Close()
	}
	// Change to the repository directory
	if err := os.Chdir(repoPath); err != nil {
		f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString(fmt.Sprintf("Failed to chdir: %v\n", err))
			f.Close()
		}
		return HandleVErrAndExitCode(errhand.BuildDError("cannot access repository at %s", repoPath).AddCause(err).Build(), usage)
	}
	// Create a new filesystem for the new directory
	fs, err := filesys.LocalFilesysWithWorkingDir(".")
	if err != nil {
		f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString(fmt.Sprintf("Failed to create filesystem: %v\n", err))
			f.Close()
		}
		return 1
	}
	// Reload environment in the new directory
	dEnv = env.Load(ctx, env.GetCurrentUserHomeDir, fs, doltdb.LocalDirDoltDB, "dolt")
	f, _ = os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f != nil {
		f.WriteString(fmt.Sprintf("Environment loaded, HasDoltDataDir: %v\n", dEnv.HasDoltDataDir()))
		f.Close()
	}

	// Load the database
	f3, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f3 != nil {
		f3.WriteString("About to load DoltDB...\n")
		f3.Close()
	}
	ddb := dEnv.DoltDB(ctx)
	if ddb == nil || dEnv.DBLoadError != nil {
		// Write to stderr for debugging (not stdout!)
		f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f != nil {
			if dEnv.DBLoadError != nil {
				f.WriteString(fmt.Sprintf("DBLoadError: %v\n", dEnv.DBLoadError))
			} else {
				f.WriteString("DoltDB is nil\n")
			}
			f.Close()
		}
		if dEnv.DBLoadError != nil {
			return HandleVErrAndExitCode(errhand.BuildDError("failed to load database").AddCause(dEnv.DBLoadError).Build(), usage)
		}
		return HandleVErrAndExitCode(errhand.BuildDError("failed to load database").Build(), usage)
	}
	
	f4, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f4 != nil {
		f4.WriteString("DoltDB loaded successfully\n")
		f4.Close()
	}

	// Create SMUX session over stdio for multiplexing
	stdioConn := &stdioConn{r: os.Stdin, w: os.Stdout}
	smuxConfig := smux.DefaultConfig()
	smuxConfig.MaxReceiveBuffer = 128 * 1024 * 1024
	smuxConfig.MaxStreamBuffer = 128 * 1024 * 1024
	
	session, err := smux.Server(stdioConn, smuxConfig)
	if err != nil {
		f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString(fmt.Sprintf("Failed to create SMUX session: %v\n", err))
			f.Close()
		}
		return 1
	}
	defer session.Close()

	// Create a gRPC server for the remote API
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(128*1024*1024),
		grpc.MaxSendMsgSize(128*1024*1024),
	)

	// Get the chunk store from the database
	db := doltdb.HackDatasDatabaseFromDoltDB(ddb)
	cs := datas.ChunkStoreFromDatabase(db)
	dbCache := &singletonDBCache{cs: cs.(remotesrv.RemoteSrvStore)}

	// Create and register the chunk store service
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	logEntry := logrus.NewEntry(logger)

	// Use identity sealer since we don't need URL sealing for local stdio transport
	sealer := &identitySealer{}
	// Use virtual host for HTTP - will be served by our embedded HTTP server
	chunkStoreService := remotesrv.NewHttpFSBackedChunkStore(logEntry, "transfer.local", dbCache, dEnv.FS, "http", remotesapi.PushConcurrencyControl_PUSH_CONCURRENCY_CONTROL_UNSPECIFIED, sealer)

	remotesapi.RegisterChunkStoreServiceServer(grpcServer, chunkStoreService)

	// Create HTTP server for serving chunk files
	// We need to create our own handler since newFileHandler is not exported
	httpHandler := &fileHandler{
		dbCache: dbCache,
		fs:      dEnv.FS,
		lgr:     logEntry,
		sealer:  sealer,
	}
	httpServer := &http.Server{
		Handler: httpHandler,
	}

	// Create listeners for both gRPC and HTTP on SMUX streams
	grpcListener := &smuxListener{session: session, name: "grpc"}
	httpListener := &smuxListener{session: session, name: "http"}
	
	f5, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f5 != nil {
		f5.WriteString("About to start gRPC server...\n")
		f5.Close()
	}
	
	// Add a goroutine to monitor if we're still running
	go func() {
		for i := 0; i < 10; i++ {
			time.Sleep(1 * time.Second)
			f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if f != nil {
				f.WriteString(fmt.Sprintf("Server still running at %v (iteration %d)\n", time.Now(), i))
				f.Close()
			}
		}
	}()
	
	// Serve gRPC directly over stdio
	f8, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f8 != nil {
		f8.WriteString("Calling grpcServer.Serve...\n")
		f8.Close()
	}
	
	// Start both servers in goroutines
	errCh := make(chan error, 2)
	
	go func() {
		f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString("Starting gRPC server...\n")
			f.Close()
		}
		if err := grpcServer.Serve(grpcListener); err != nil {
			errCh <- fmt.Errorf("gRPC server error: %w", err)
		}
	}()
	
	go func() {
		f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString("Starting HTTP server...\n")
			f.Close()
		}
		if err := httpServer.Serve(httpListener); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("HTTP server error: %w", err)
		}
	}()
	
	// Wait for either server to error, session to close, or context to be done
	select {
	case err := <-errCh:
		f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString(fmt.Sprintf("Server error: %v\n", err))
			f.Close()
		}
		return 1
	case <-session.CloseChan():
		f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString("SMUX session closed\n")
			f.Close()
		}
		return 0
	case <-ctx.Done():
		f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString(fmt.Sprintf("Context done: %v\n", ctx.Err()))
			f.Close()
		}
		return 0
	}
}

// oneConnListener implements net.Listener that returns exactly one connection
type oneConnListener struct {
	conn net.Conn
	once sync.Once
	done chan struct{}
}

func newOneConnListener(conn net.Conn) *oneConnListener {
	return &oneConnListener{conn: conn, done: make(chan struct{})}
}

func (l *oneConnListener) Accept() (net.Conn, error) {
	f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f != nil {
		f.WriteString(fmt.Sprintf("Accept called at %v\n", time.Now()))
		f.Close()
	}
	
	var c net.Conn
	l.once.Do(func() { 
			c = l.conn 
			f2, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if f2 != nil {
				f2.WriteString("oneConnListener.Accept returning connection (first call)\n")
				f2.Close()
			}
	})
	if c != nil {
		// Monitor what happens after we return the connection
		go func() {
			for i := 0; i < 5; i++ {
				time.Sleep(100 * time.Millisecond)
				f3, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
				if f3 != nil {
					f3.WriteString(fmt.Sprintf("After Accept return: %dms\n", (i+1)*100))
					f3.Close()
				}
			}
		}()
		return c, nil
	}
	f5, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f5 != nil {
		f5.WriteString("Accept: About to block on <-l.done\n")
		f5.Close()
	}
	<-l.done
	f4, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f4 != nil {
		f4.WriteString("oneConnListener.Accept returning net.ErrClosed\n")
		f4.Close()
	}
	return nil, net.ErrClosed
}

func (l *oneConnListener) Close() error {
	select {
	case <-l.done:
	default:
		close(l.done)
	}
	return nil
}

func (l *oneConnListener) Addr() net.Addr {
	return &stdioAddr{}
}

// singletonDBCache implements remotesrv.DBCache for a single database
type singletonDBCache struct {
	cs remotesrv.RemoteSrvStore
}

func (s *singletonDBCache) Get(ctx context.Context, path, nbfVerStr string) (remotesrv.RemoteSrvStore, error) {
	return s.cs, nil
}

// fileHandler serves chunk files over HTTP
type fileHandler struct {
	dbCache remotesrv.DBCache
	fs      filesys.Filesys
	lgr     *logrus.Entry
	sealer  remotesrv.Sealer
}

func (fh *fileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Log the request to debug file
	f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f != nil {
		f.WriteString(fmt.Sprintf("HTTP request: %s %s\n", r.Method, r.URL.Path))
		f.Close()
	}
	
	// Unseal the URL if needed
	url, err := fh.sealer.Unseal(r.URL)
	if err != nil {
		fh.lgr.WithError(err).Warn("could not unseal URL")
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	
	path := strings.TrimLeft(url.Path, "/")
	
	// Handle different HTTP methods
	switch r.Method {
	case http.MethodGet:
		fh.handleGet(w, r, path)
		
	case http.MethodPost, http.MethodPut:
		fh.handleUpload(w, r, path)
		
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func (fh *fileHandler) handleGet(w http.ResponseWriter, r *http.Request, path string) {
	// For SSH transport, the path includes the database path
	// Just use the full path as the file path
	filePath := path
	
	// Read the file from the filesystem
	// The paths are in the format: .dolt/noms/XXXX
	f2, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f2 != nil {
		f2.WriteString(fmt.Sprintf("Trying to open file: %s\n", filePath))
		f2.Close()
	}
	
	reader, err := fh.fs.OpenForRead(filePath)
	if err != nil {
		f3, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f3 != nil {
			f3.WriteString(fmt.Sprintf("File not found: %s (error: %v)\n", filePath, err))
			f3.Close()
		}
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	defer reader.Close()
	
	// Check if this is a range request
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		// Parse the range header (format: bytes=start-end)
		var start, end int64
		if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err == nil {
			// Read the full file to get its size
			data, err := io.ReadAll(reader)
			if err != nil {
				fh.lgr.WithError(err).Error("failed to read file")
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			
			fileSize := int64(len(data))
			
			// Validate range
			if start < 0 || start >= fileSize || end >= fileSize || start > end {
				http.Error(w, "Requested Range Not Satisfiable", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			
			// Extract the requested range
			rangeData := data[start:end+1]
			
			// Log what we're sending
			f4, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if f4 != nil {
				f4.WriteString(fmt.Sprintf("Sending range %d-%d of %s, size: %d\n", start, end, filePath, len(rangeData)))
				f4.Close()
			}
			
			// Send partial content response
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(rangeData)))
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(rangeData)
			return
		}
	}
	
	// No range request, send the full file
	data, err := io.ReadAll(reader)
	if err != nil {
		fh.lgr.WithError(err).Error("failed to read file")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	
	// Log the checksum of what we're sending
	checksum := md5.Sum(data)
	f4, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f4 != nil {
		f4.WriteString(fmt.Sprintf("Sending full file: %s, size: %d, md5: %x\n", filePath, len(data), checksum))
		f4.Close()
	}
	
	// Serve the file content
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.Write(data)
}

func (fh *fileHandler) handleUpload(w http.ResponseWriter, r *http.Request, path string) {
	// Log the upload request
	f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f != nil {
		f.WriteString(fmt.Sprintf("Upload request: %s %s\n", r.Method, path))
		f.Close()
	}
	
	// Extract the file name from the path
	i := strings.LastIndex(path, "/")
	if i < 0 {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	
	filepath := path[:i]
	filename := path[i+1:]
	
	// Parse query parameters
	q := r.URL.Query()
	
	numChunksStr := q.Get("num_chunks")
	if numChunksStr == "" {
		f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString("Missing num_chunks parameter\n")
			f.Close()
		}
		http.Error(w, "Bad Request: num_chunks parameter required", http.StatusBadRequest)
		return
	}
	numChunks, err := strconv.Atoi(numChunksStr)
	if err != nil {
		http.Error(w, "Bad Request: invalid num_chunks", http.StatusBadRequest)
		return
	}
	
	contentLengthStr := q.Get("content_length")
	if contentLengthStr == "" {
		f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString("Missing content_length parameter\n")
			f.Close()
		}
		http.Error(w, "Bad Request: content_length parameter required", http.StatusBadRequest)
		return
	}
	contentLength, err := strconv.ParseUint(contentLengthStr, 10, 64)
	if err != nil {
		http.Error(w, "Bad Request: invalid content_length", http.StatusBadRequest)
		return
	}
	
	contentHashStr := q.Get("content_hash")
	if contentHashStr == "" {
		f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString("Missing content_hash parameter\n")
			f.Close()
		}
		http.Error(w, "Bad Request: content_hash parameter required", http.StatusBadRequest)
		return
	}
	contentHash, err := base64.RawURLEncoding.DecodeString(contentHashStr)
	if err != nil {
		http.Error(w, "Bad Request: invalid content_hash", http.StatusBadRequest)
		return
	}
	
	// Parse optional split_offset parameter
	splitOffset := uint64(0)
	splitOffsetStr := q.Get("split_offset")
	if splitOffsetStr != "" {
		splitOffset, err = strconv.ParseUint(splitOffsetStr, 10, 64)
		if err != nil {
			http.Error(w, "Bad Request: invalid split_offset", http.StatusBadRequest)
			return
		}
	}
	
	// Get the chunk store from the database cache
	cs, err := fh.dbCache.Get(r.Context(), filepath, "")
	if err != nil {
		f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString(fmt.Sprintf("Failed to get chunk store: %v\n", err))
			f.Close()
		}
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	
	// Write the table file using the chunk store
	err = cs.WriteTableFile(r.Context(), filename, splitOffset, numChunks, contentHash, func() (io.ReadCloser, uint64, error) {
		return r.Body, contentLength, nil
	})
	
	if err != nil {
		f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if f != nil {
			f.WriteString(fmt.Sprintf("Failed to write table file: %v\n", err))
			f.Close()
		}
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	
	// Log successful upload
	f2, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f2 != nil {
		f2.WriteString(fmt.Sprintf("Successfully uploaded: %s/%s (chunks: %d, size: %d, hash: %x)\n", 
			filepath, filename, numChunks, contentLength, contentHash))
		f2.Close()
	}
	
	// Send success response
	w.WriteHeader(http.StatusOK)
}

// smuxListener wraps an SMUX session to implement net.Listener
type smuxListener struct {
	session *smux.Session
	name    string
}

func (l *smuxListener) Accept() (net.Conn, error) {
	stream, err := l.session.AcceptStream()
	if err != nil {
		return nil, err
	}
	f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f != nil {
		f.WriteString(fmt.Sprintf("%s listener accepted stream\n", l.name))
		f.Close()
	}
	return stream, nil
}

func (l *smuxListener) Close() error {
	return nil // Session close is handled elsewhere
}

func (l *smuxListener) Addr() net.Addr {
	return &stdioAddr{}
}

// stdioConn implements net.Conn over stdin/stdout
type stdioConn struct {
	r io.Reader
	w io.Writer
}

func (c *stdioConn) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	// Log all reads to see what's happening
	f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f != nil {
		if err != nil {
			f.WriteString(fmt.Sprintf("stdioConn.Read: n=%d, err=%v at %v\n", n, err, time.Now()))
			if err == io.EOF || err == io.ErrClosedPipe {
				// Instead of exiting, just keep blocking
				f.WriteString("Got EOF/closed pipe on stdin, blocking forever\n")
				f.Close()
				// Block forever instead of returning EOF
				select {}
			}
		} else if n > 0 {
			f.WriteString(fmt.Sprintf("stdioConn.Read: n=%d (success)\n", n))
		}
		f.Close()
	}
	return n, err
}

func (c *stdioConn) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	// Log all writes and errors
	f, _ := os.OpenFile("/tmp/transfer_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f != nil {
		if err != nil {
			f.WriteString(fmt.Sprintf("stdioConn.Write error: n=%d, err=%v at %v\n", n, err, time.Now()))
			// If we get an error writing to stdout, block forever instead of exiting
			if err == io.ErrClosedPipe || err == syscall.EPIPE {
				f.WriteString("Got closed pipe/EPIPE on stdout, blocking forever\n")
				f.Close()
				select {}
			}
		} else if n > 0 {
			f.WriteString(fmt.Sprintf("stdioConn.Write: n=%d (success)\n", n))
		}
		f.Close()
	}
	return n, err
}

func (c *stdioConn) Close() error {
	// Don't close stdin/stdout
	return nil
}

func (c *stdioConn) LocalAddr() net.Addr                { return &stdioAddr{} }
func (c *stdioConn) RemoteAddr() net.Addr               { return &stdioAddr{} }
func (c *stdioConn) SetDeadline(t time.Time) error      { return nil }
func (c *stdioConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *stdioConn) SetWriteDeadline(t time.Time) error { return nil }

// stdioAddr implements net.Addr for stdio connections
type stdioAddr struct{}

func (a *stdioAddr) Network() string { return "stdio" }
func (a *stdioAddr) String() string  { return "stdio" }

