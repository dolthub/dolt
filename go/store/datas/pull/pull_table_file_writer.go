// Copyright 2024 Dolthub, Inc.
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

package pull

import (
	"context"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/nbs"
)

// A PullTableFileWriter is used by the Puller to manage comprsesed table file
// writers for a pull or push process. It manages writing compressed chunks
// into table files and adding them to the sink database for a Pull.
//
// It can be configured with:
// * Target file size for uploaded table files.
// * Number of concurrent table file uploads.
// * Number of pending table files awaiting upload.
//
// For the last configuration point, the basic observation is that pushes are
// not currently resumable across `dolt push`/`call dolt_push` invocations.  It
// is not necessarily in a user's best interest to buffer lots and lots of
// table files to the local disk while a user awaits the upload of the existing
// buffered table files to the remote database. In the worst case, it can cause
// 2x disk utilization on a pushing host, which is not what the user expects.
//
// Note that, as currently implemented, the limit on the number of pending
// table files applies to table files which are not being uploaded at all
// currently. So the total number of table files possible is # of concurrent
// uploads + number of pending table files.
//
// A PullTableFileWriter needs must be |Close()|d at the end of delivering all
// of its chunks, since it needs to finalize the last in-flight table file and
// finish uploading all remaining table files. The error from |Close()| must be
// checked, since it will include any failure to upload the files.
type PullTableFileWriter struct {
	cfg PullTableFileWriterConfig

	addChunkCh  chan nbs.ToChunker
	newWriterCh chan nbs.GenericTableWriter
	egCtx       context.Context
	eg          *errgroup.Group

	getAddrs chunks.GetAddrsCurry

	bufferedSendBytes uint64
	finishedSendBytes uint64
}

type PullTableFileWriterConfig struct {
	ConcurrentUploads int

	ChunksPerFile int

	MaximumBufferedFiles int

	TempDir string

	DestStore DestTableFileStore

	GetAddrs chunks.GetAddrsCurry
}

type DestTableFileStore interface {
	WriteTableFile(ctx context.Context, id string, numChunks int, contentHash []byte, getRd func() (io.ReadCloser, uint64, error)) error
	AddTableFilesToManifest(ctx context.Context, fileIdToNumChunks map[string]int, getAddrs chunks.GetAddrsCurry) error
}

type PullTableFileWriterStats struct {
	// Bytes which are queued up to be sent to the destination but have not
	// yet gone out on the wire.
	BufferedSendBytes uint64

	// Bytes which we sent to the destination. These have been delivered to
	// the operating system to be sent to the destination database. This
	// number never goes down. In the case that we have to retry an upload,
	// for example, BufferedSendBytes will instead go up.
	FinishedSendBytes uint64
}

func NewPullTableFileWriter(ctx context.Context, cfg PullTableFileWriterConfig) *PullTableFileWriter {
	ret := &PullTableFileWriter{
		cfg:         cfg,
		addChunkCh:  make(chan nbs.ToChunker),
		newWriterCh: make(chan nbs.GenericTableWriter, cfg.MaximumBufferedFiles),
		getAddrs:    cfg.GetAddrs,
	}
	ret.eg, ret.egCtx = errgroup.WithContext(ctx)
	ret.eg.Go(ret.uploadAndFinalizeThread)
	ret.eg.Go(ret.addChunkThread)
	return ret
}

func (w *PullTableFileWriter) GetStats() PullTableFileWriterStats {
	return PullTableFileWriterStats{
		FinishedSendBytes: atomic.LoadUint64(&w.finishedSendBytes),
		BufferedSendBytes: atomic.LoadUint64(&w.bufferedSendBytes),
	}
}

// Adds the compressed chunk to the table files to be uploaded to the destination store.
//
// If there is a terminal error with uploading a table file, this method will
// start returning a non-nil |error|.
//
// This method may block for arbitrary amounts of time if there is already a
// lot of buffered table files and we are waiting for uploads to succeed before
// creating more table files.
func (w *PullTableFileWriter) AddToChunker(ctx context.Context, chk nbs.ToChunker) error {
	select {
	case w.addChunkCh <- chk:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-w.egCtx.Done():
		return w.eg.Wait()
	}
}

// This thread coordinates the threads which do the actual uploading. It spawns
// the appropriate number of upload threads and waits for them all to exit,
// which they typically do after newWriterCh is closed, but may also do if
// their context is canceled. This thread reads the response channel from the
// upload threads and accumulate the manifest updates.
//
// When all uploads threads have exited, it finishes reading the response
// channel, and then, if all upload threads exited successfully, it applies any
// necessary manifest updates to the DestStore.
func (w *PullTableFileWriter) uploadAndFinalizeThread() (err error) {
	respCh := make(chan tempTblFile)

	uploadEg, uploadCtx := errgroup.WithContext(w.egCtx)
	for i := 0; i < w.cfg.ConcurrentUploads; i++ {
		uploadEg.Go(func() error {
			return w.uploadThread(uploadCtx, w.newWriterCh, respCh)
		})
	}

	// After all upload threads are done, the response channel is closed.
	go func() {
		uploadEg.Wait()
		close(respCh)
	}()

	// We don't need too much coordination here, since respCh is guaranteed
	// to always be closed after uploadEg is done and we are going to check
	// for errors later.
	manifestUpdates := make(map[string]int)
	var manifestWg sync.WaitGroup
	manifestWg.Add(1)
	go func() {
		defer manifestWg.Done()
		for ttf := range respCh {
			id := ttf.id
			if strings.HasSuffix(id, nbs.ArchiveFileSuffix) {
				id = id[:len(id)-len(nbs.ArchiveFileSuffix)]
			}

			manifestUpdates[id] = ttf.numChunks
		}
	}()

	manifestWg.Wait()
	err = uploadEg.Wait()
	if err != nil {
		return err
	}

	if len(manifestUpdates) > 0 {
		return w.cfg.DestStore.AddTableFilesToManifest(w.egCtx, manifestUpdates, w.getAddrs)
	} else {
		return nil
	}
}

// This thread reads from addChunkCh and writes the chunks to table files.
// When a table file gets big enough, it stops reading from addChunkCh
// temporarily and shuffles the file off to the pendingUploadThread, before it
// goes back to reading from addChunkCh.
//
// Once addChunkCh closes, it sends along the last table file, if any, and then
// closes newWriterCh and exits itself.
func (w *PullTableFileWriter) addChunkThread() (err error) {
	var curWr nbs.GenericTableWriter

	defer func() {
		if curWr != nil {
			// Cleanup dangling writer, whose contents will never be used.
			_, _ = curWr.Finish()
			rd, _ := curWr.Reader()
			if rd != nil {
				rd.Close()
			}
		}
	}()

	sendTableFile := func() error {
		select {
		case <-w.egCtx.Done():
			return context.Cause(w.egCtx)
		case w.newWriterCh <- curWr:
			curWr = nil
			return nil
		}
	}

LOOP:
	for {
		if curWr != nil && curWr.ChunkCount() >= w.cfg.ChunksPerFile {
			if err := sendTableFile(); err != nil {
				return err
			}
			continue
		}

		select {
		case <-w.egCtx.Done():
			return context.Cause(w.egCtx)
		case newChnk, ok := <-w.addChunkCh:
			if !ok {
				break LOOP
			}

			if curWr == nil {
				if os.Getenv("DOLT_ARCHIVE_PULL_STREAMER") != "" {
					curWr, err = nbs.NewArchiveStreamWriter(w.cfg.TempDir)
				} else {
					curWr, err = nbs.NewCmpChunkTableWriter(w.cfg.TempDir)
				}
				if err != nil {
					return err
				}
			}

			// Add the chunk to writer.
			bytes, err := curWr.AddChunk(newChnk)
			if err != nil {
				return err
			}

			atomic.AddUint64(&w.bufferedSendBytes, uint64(bytes))
		}
	}

	// Send the last writer, if there is one.
	if curWr != nil {
		if err := sendTableFile(); err != nil {
			return err
		}
	}

	close(w.newWriterCh)

	return nil
}

// Finalize any in-flight table file writes and add all the uploaded table
// files to the destination database.
//
// Returns any errors encountered on uploading or adding the table files to the
// destination database.
func (w *PullTableFileWriter) Close() error {
	close(w.addChunkCh)
	return w.eg.Wait()
}

func (w *PullTableFileWriter) uploadThread(ctx context.Context, reqCh chan nbs.GenericTableWriter, respCh chan tempTblFile) error {
	for {
		select {
		case wr, ok := <-reqCh:
			if !ok {
				return nil
			}
			// content length before we finish the write, which will
			// add the index and table file footer.
			chunksLen := wr.ContentLength()

			id, err := wr.Finish()
			if err != nil {
				return err
			}

			ttf := tempTblFile{
				id:          id,
				read:        wr,
				numChunks:   wr.ChunkCount(),
				chunksLen:   chunksLen,
				contentLen:  wr.ContentLength(),
				contentHash: wr.GetMD5(),
			}
			err = w.uploadTempTableFile(ctx, ttf)

			// Always remove the file...
			wr.Remove()

			if err != nil {
				return err
			}

			select {
			case respCh <- ttf:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

func (w *PullTableFileWriter) uploadTempTableFile(ctx context.Context, tmpTblFile tempTblFile) error {
	fileSize := tmpTblFile.contentLen

	// So far, we've added all the bytes for the compressed chunk data.
	// We add the remaining bytes here --- bytes for the index and the
	// table file footer.
	atomic.AddUint64(&w.bufferedSendBytes, uint64(fileSize)-tmpTblFile.chunksLen)

	// Tracks the number of bytes we have uploaded as part of a ReadCloser() get from a WriteTableFile call.
	// If the upload get retried by WriteTableFile, then the callback gets
	// called more than once, and we can register it as rebuffering the
	// already upload bytes.
	var uploaded uint64

	return w.cfg.DestStore.WriteTableFile(ctx, tmpTblFile.id, tmpTblFile.numChunks, tmpTblFile.contentHash, func() (io.ReadCloser, uint64, error) {
		rc, err := tmpTblFile.read.Reader()
		if err != nil {
			return nil, 0, err
		}

		if uploaded != 0 {
			// A retry. We treat it as if what was already uploaded was rebuffered.
			atomic.AddUint64(&w.bufferedSendBytes, uint64(uploaded))
			uploaded = 0
		}

		fWithStats := countingReader{countingReader{rc, &uploaded}, &w.finishedSendBytes}

		return fWithStats, uint64(fileSize), nil
	})
}
