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
	"sync/atomic"

	"golang.org/x/sync/errgroup"

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

	addChunkCh chan nbs.CompressedChunk
	egCtx      context.Context
	eg         *errgroup.Group

	bufferedSendBytes uint64
	finishedSendBytes uint64
}

type PullTableFileWriterConfig struct {
	ConcurrentUploads int

	ChunksPerFile int

	MaximumBufferedFiles int

	TempDir string

	DestStore DestTableFileStore
}

type DestTableFileStore interface {
	WriteTableFile(ctx context.Context, id string, numChunks int, contentHash []byte, getRd func() (io.ReadCloser, uint64, error)) error
	AddTableFilesToManifest(ctx context.Context, fileIdToNumChunks map[string]int) error
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
		cfg:        cfg,
		addChunkCh: make(chan nbs.CompressedChunk),
	}
	ret.eg, ret.egCtx = errgroup.WithContext(ctx)
	ret.eg.Go(ret.reqRespThread)
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
func (w *PullTableFileWriter) AddCompressedChunk(ctx context.Context, chk nbs.CompressedChunk) error {
	select {
	case w.addChunkCh <- chk:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-w.egCtx.Done():
		return w.eg.Wait()
	}
}

func (w *PullTableFileWriter) reqRespThread() (err error) {
	reqCh := make(chan uploadReq)
	respCh := make(chan tempTblFile)
	manifestUpdates := make(map[string]int)

	// Uploads that haven't been sent to a request thread yet.
	var pendingUploads []uploadReq
	// The count of uploads that have been accepted by request threads.
	// Must reach 0 for uploads to be completed and the manifest update to
	// proceed.
	outstandingUploads := 0

	// After the writer is closed, it stops accepting new chunks and waits
	// for all in-flight uploads to complete.
	closed := false

	eg, ctx := errgroup.WithContext(w.egCtx)
	for i := 0; i < w.cfg.ConcurrentUploads; i++ {
		eg.Go(func() error {
			return w.uploadThread(ctx, reqCh, respCh)
		})
	}

	var curWr *nbs.CmpChunkTableWriter

	defer func() {
		close(reqCh)
		egErr := eg.Wait()
		if err == nil {
			err = egErr
		}

		if curWr != nil {
			// Cleanup dangling writer, whose contents will never be used.
			curWr.Finish()
			rd, _ := curWr.Reader()
			if rd != nil {
				rd.Close()
			}
		}
	}()

	for {
		if closed && len(pendingUploads) == 0 && outstandingUploads == 0 {
			break
		}

		addChunkCh := w.addChunkCh
		if closed || len(pendingUploads) > w.cfg.MaximumBufferedFiles {
			// If we are already closed, or we have too many
			// pending uploads already, stop accepting new chunks
			// for now.
			addChunkCh = nil
		}

		// Send the next pending upload to an upload thread.
		var thisReqCh chan uploadReq
		var pendingUpload uploadReq
		if len(pendingUploads) > 0 {
			pendingUpload = pendingUploads[len(pendingUploads)-1]
			thisReqCh = reqCh
		}

		select {
		case <-ctx.Done():
			return eg.Wait()
		case <-w.egCtx.Done():
			return context.Cause(w.egCtx)
		case thisReqCh <- pendingUpload:
			// Keep track of how many responses we need to wait for.
			outstandingUploads += 1
			pendingUploads[len(pendingUploads)-1] = uploadReq{}
			pendingUploads = pendingUploads[:len(pendingUploads)-1]
		case resp := <-respCh:
			// A file was uploaded successfully - record it in the pending manifest updates.
			outstandingUploads -= 1
			manifestUpdates[resp.id] = resp.numChunks
		case newChnk, ok := <-addChunkCh:
			if !ok {
				// Close() was called. Upload the inflight file if necessary.
				closed = true
				if curWr != nil {
					pendingUploads = append(pendingUploads, uploadReq{
						wr: curWr,
					})
					curWr = nil
				}
				continue
			}

			if curWr == nil {
				curWr, err = nbs.NewCmpChunkTableWriter(w.cfg.TempDir)
				if err != nil {
					return err
				}
			}

			// Add the chunk to writer.
			err = curWr.AddCmpChunk(newChnk)
			if err != nil {
				return err
			}
			atomic.AddUint64(&w.bufferedSendBytes, uint64(len(newChnk.FullCompressedChunk)))

			// Possibly finalize writer into pendingUpload.
			if curWr.ChunkCount() >= w.cfg.ChunksPerFile {
				pendingUploads = append(pendingUploads, uploadReq{
					wr: curWr,
				})
				curWr = nil
			}
		}
	}

	if len(manifestUpdates) > 0 {
		err := w.cfg.DestStore.AddTableFilesToManifest(w.egCtx, manifestUpdates)
		if err != nil {
			return err
		}
	}

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

type uploadReq struct {
	wr *nbs.CmpChunkTableWriter
}

func (w *PullTableFileWriter) uploadThread(ctx context.Context, reqCh chan uploadReq, respCh chan tempTblFile) error {
	for {
		select {
		case req, ok := <-reqCh:
			if !ok {
				return nil
			}
			wr := req.wr

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
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
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
