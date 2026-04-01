// Copyright 2019 Dolthub, Inc.
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
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

var ErrNoData = errors.New("no data")
var ErrCloneUnsupported = errors.New("clone unsupported")

func Clone(ctx context.Context, srcCS, sinkCS chunks.ChunkStore, getAddrs chunks.InsertAddrsCurry, tempTableDir string, eventCh chan<- TableFileEvent) error {
	srcTS, srcOK := srcCS.(chunks.TableFileStore)

	if !srcOK {
		return fmt.Errorf("%w: src db is not a Table File Store", ErrCloneUnsupported)
	}

	size, err := srcTS.Size(ctx)

	if err != nil {
		return err
	}

	if size == 0 {
		return ErrNoData
	}

	sinkTS, sinkOK := sinkCS.(chunks.TableFileStore)

	if !sinkOK {
		return fmt.Errorf("%w: sink db is not a Table File Store", ErrCloneUnsupported)
	}

	return clone(ctx, srcTS, sinkTS, sinkCS, getAddrs, tempTableDir, eventCh)
}

type CloneTableFileEvent int

const (
	Listed = iota
	DownloadStart
	DownloadStats
	DownloadSuccess
	DownloadFailed
)

type TableFileEvent struct {
	TableFiles []chunks.TableFile
	Stats      []iohelp.ReadStats
	EventType  CloneTableFileEvent
}

// mapTableFiles returns the list of all fileIDs for the table files, and a map from fileID to chunks.TableFile
func mapTableFiles(tblFiles []chunks.TableFile) ([]string, map[string]chunks.TableFile, map[string]int) {
	fileIds := make([]string, len(tblFiles))
	fileIDtoTblFile := make(map[string]chunks.TableFile)
	fileIDtoNumChunks := make(map[string]int)

	for i, tblFile := range tblFiles {
		fileId := tblFile.FileID()

		fileIDtoTblFile[fileId] = tblFile
		fileIds[i] = fileId
		fileIDtoNumChunks[fileId] = tblFile.NumChunks()
	}

	return fileIds, fileIDtoTblFile, fileIDtoNumChunks
}

const concurrentTableFileDownloads = 3

func clone(ctx context.Context, srcTS, sinkTS chunks.TableFileStore, sinkCS chunks.ChunkStore, getAddrs chunks.InsertAddrsCurry, tempTableDir string, eventCh chan<- TableFileEvent) error {
	sources, err := srcTS.Sources(ctx)
	if err != nil {
		return err
	}
	root := sources.Root
	sourceFiles := sources.TableFiles
	appendixFiles := sources.AppendixTableFiles

	tblFiles := filterAppendicesFromSourceFiles(appendixFiles, sourceFiles)
	report := func(e TableFileEvent) {
		if eventCh != nil {
			eventCh <- e
		}
	}

	// Initializes the list of fileIDs we are going to download, and the map of fileIDToTF.  If this clone takes a long
	// time some of the urls within the chunks.TableFiles will expire and fail to download.  At that point we will retrieve
	// the sources again, and update the fileIDToTF map with updated info, but not change the files we are downloading.
	desiredFiles, fileIDToTF, fileIDToNumChunks := mapTableFiles(tblFiles)
	completed := make([]bool, len(desiredFiles))

	report(TableFileEvent{EventType: Listed, TableFiles: tblFiles})

	download := func(ctx context.Context) error {
		sem := semaphore.NewWeighted(concurrentTableFileDownloads)
		eg, ctx := errgroup.WithContext(ctx)
		for i := range desiredFiles {
			if completed[i] {
				continue
			}
			if err := sem.Acquire(ctx, 1); err != nil {
				// The errgroup ctx has been canceled. We will
				// return the error from wg.Wait() below.
				break
			}
			eg.Go(func() (err error) {
				defer sem.Release(1)

				fileID := desiredFiles[i]
				tblFile, ok := fileIDToTF[fileID]
				if !ok {
					// conjoin happened during clone
					return backoff.Permanent(errors.New("table file not found. please try again"))
				}

				report(TableFileEvent{EventType: DownloadStart, TableFiles: []chunks.TableFile{tblFile}})

				// XXX: This is not the best place to do this conversion.
				// Some issues:
				// 1) We have to reconvert the file if this conversion gets retried.
				// 2) The stats we post through rdStats only reflect the initial read, instead
				// of reflecting the read + (approximate) upload progress, which the other
				// branch handles.
				if tblFile.FileID() == chunks.JournalFileID {
					rd, contentLength, err := tblFile.Open(ctx)
					if err != nil {
						return err
					}
					rdStats := iohelp.NewReaderWithStats(rd, int64(contentLength))
					rdStats.Start(func(s iohelp.ReadStats) {
						report(TableFileEvent{
							EventType:  DownloadStats,
							TableFiles: []chunks.TableFile{tblFile},
							Stats:      []iohelp.ReadStats{s},
						})
					})
					writer, uploadFileID, err := convertJournalToTableFile(ctx, rdStats, 0, tempTableDir)
					rdStats.Close()
					if err != nil {
						return err
					}
					defer writer.Cancel()
					splitOff, err := writer.ChunkDataLength()
					if err != nil {
						return err
					}
					err = sinkTS.WriteTableFile(ctx, uploadFileID, splitOff, writer.ChunkCount(), writer.GetMD5(), func() (io.ReadCloser, uint64, error) {
						rdr, err := writer.Reader()
						if err != nil {
							return nil, 0, err
						}
						return rdr, writer.FullLength(), nil
					})
					if err != nil {
						report(TableFileEvent{EventType: DownloadFailed, TableFiles: []chunks.TableFile{tblFile}})
						return err
					} else {
						report(TableFileEvent{EventType: DownloadSuccess, TableFiles: []chunks.TableFile{tblFile}})
						completed[i] = true

						// XXX: A gross hack. fileIDToNumChunks is our input to AddTableFiles. Update it here
						// so we add the uploaded archive file to the store, not a non-existant table file with
						// the `vvv...` name.
						//
						// We can mutate fileIDToNumChunks safely here because we are the only goroutine
						// which will ever mutate it and it is not read until after the eg.Wait() on the errgroup
						// in which we are running.
						delete(fileIDToNumChunks, chunks.JournalFileID)
						fileIDToNumChunks[strings.TrimSuffix(uploadFileID, nbs.ArchiveFileSuffix)] = writer.ChunkCount()

						return nil
					}
				} else {
					uploadFileID := tblFile.FileID() + tblFile.LocationSuffix()
					splitOff := tblFile.SplitOffset()
					numChunks := tblFile.NumChunks()
					err = sinkTS.WriteTableFile(ctx, uploadFileID, splitOff, numChunks, nil, func() (io.ReadCloser, uint64, error) {
						rd, contentLength, err := tblFile.Open(ctx)
						if err != nil {
							return nil, 0, err
						}
						rdStats := iohelp.NewReaderWithStats(rd, int64(contentLength))

						rdStats.Start(func(s iohelp.ReadStats) {
							report(TableFileEvent{
								EventType:  DownloadStats,
								TableFiles: []chunks.TableFile{tblFile},
								Stats:      []iohelp.ReadStats{s},
							})
						})

						return rdStats, contentLength, nil
					})
					if err != nil {
						report(TableFileEvent{EventType: DownloadFailed, TableFiles: []chunks.TableFile{tblFile}})
						return err
					} else {
						report(TableFileEvent{EventType: DownloadSuccess, TableFiles: []chunks.TableFile{tblFile}})
						completed[i] = true
						return nil
					}
				}
			})
		}

		return eg.Wait()
	}

	const maxAttempts = 3
	previousCompletedCnt := 0
	failureCount := 0

	madeProgress := func() bool {
		currentCompletedCnt := 0
		for _, b := range completed {
			if b {
				currentCompletedCnt++
			}
		}
		if currentCompletedCnt == previousCompletedCnt {
			return false
		} else {
			previousCompletedCnt = currentCompletedCnt
			return true
		}
	}

	// keep going as long as progress is being made.  If progress is not made retry up to maxAttempts times.
	for {
		err = download(ctx)
		if err == nil {
			break
		}
		if permanent, ok := err.(*backoff.PermanentError); ok {
			return permanent.Err
		} else if madeProgress() {
			failureCount = 0
		} else {
			failureCount++
		}
		if failureCount >= maxAttempts {
			return err
		}
		if refreshSources, err := srcTS.Sources(ctx); err != nil {
			return err
		} else {
			refreshedTblFiles := filterAppendicesFromSourceFiles(refreshSources.AppendixTableFiles, refreshSources.TableFiles)
			_, refreshedFileIDToTF, _ := mapTableFiles(refreshedTblFiles)
			// Sources() will refresh remote table file
			// sources with new download URLs. However, it
			// will only return URLs for table files which
			// are in the remote manifest, which could
			// have changed since the clone started. Here
			// we keep around any old TableFile instances
			// for any TableFiles which have been
			// conjoined away or have been the victim of a
			// garbage collection run on the remote.
			//
			// If these files are no longer accessible,
			// for example because the URLs expired
			// without a RefreshTableFileUrlRequest being
			// provided, or because the table files
			// themselves have been removed from storage,
			// then continuing to use these sources will
			// fail terminally eventually. But in the
			// case of doltremoteapi on DoltHub, using
			// these Sources() will continue to work and
			// will allow the Clone to proceed.
			for k, v := range refreshedFileIDToTF {
				fileIDToTF[k] = v
			}
		}
	}

	err = sinkTS.AddTableFilesToManifest(ctx, fileIDToNumChunks, getAddrs)
	if err != nil {
		return err
	}

	// AddTableFilesToManifest can set the root chunk if there is a chunk
	// journal which we downloaded in the clone. If that happened, the
	// chunk journal is actually more accurate on what the current root is
	// than the result of |Sources| up above. We choose not to touch
	// anything in that case.
	err = sinkCS.Rebase(ctx)
	if err != nil {
		return err
	}
	sinkRoot, err := sinkCS.Root(ctx)
	if err != nil {
		return err
	}
	if !sinkRoot.IsEmpty() {
		return nil
	}

	success, err := sinkTS.Commit(ctx, root, hash.Hash{})
	if !success && err == nil {
		return errors.New("root update failure. optimistic lock failed")
	}
	if success && err != nil {
		panic(fmt.Sprintf("runtime error: successful root update with error: %v", err))
	}
	return err
}

func convertJournalToTableFile(ctx context.Context, readCloser io.ReadCloser, off int64, tmpDir string) (*nbs.ArchiveStreamWriter, string, error) {
	writer, err := nbs.NewArchiveStreamWriter(tmpDir)
	if err != nil {
		return nil, "", err
	}
	err = nbs.VisitJournalReaderChunks(ctx, readCloser, off, func(cb nbs.CompressedChunk) error {
		_, err := writer.AddChunk(cb)
		return err
	})
	if err != nil {
		return nil, "", errors.Join(err, writer.Cancel())
	}
	_, name, err := writer.Finish()
	if err != nil {
		return nil, "", errors.Join(err, writer.Cancel())
	}
	return writer, name, nil
}

func filterAppendicesFromSourceFiles(appendixFiles []chunks.TableFile, sourceFiles []chunks.TableFile) []chunks.TableFile {
	if len(appendixFiles) == 0 {
		return sourceFiles
	}
	tblFiles := make([]chunks.TableFile, 0)
	_, appendixMap, _ := mapTableFiles(appendixFiles)
	for _, sf := range sourceFiles {
		if _, ok := appendixMap[sf.FileID()]; !ok {
			tblFiles = append(tblFiles, sf)
		}
	}
	return tblFiles
}
