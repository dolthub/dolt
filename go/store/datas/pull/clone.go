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

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

var ErrNoData = errors.New("no data")
var ErrCloneUnsupported = errors.New("clone unsupported")

func Clone(ctx context.Context, srcCS, sinkCS chunks.ChunkStore, getAddrs chunks.GetAddrsCurry, eventCh chan<- TableFileEvent) error {
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

	return clone(ctx, srcTS, sinkTS, sinkCS, getAddrs, eventCh)
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
	EventType  CloneTableFileEvent
	TableFiles []chunks.TableFile
	Stats      []iohelp.ReadStats
}

// mapTableFiles returns the list of all fileIDs for the table files, and a map from fileID to chunks.TableFile
func mapTableFiles(tblFiles []chunks.TableFile) ([]string, map[string]chunks.TableFile, map[string]int) {
	fileIds := make([]string, len(tblFiles))
	fileIDtoTblFile := make(map[string]chunks.TableFile)
	fileIDtoNumChunks := make(map[string]int)

	for i, tblFile := range tblFiles {
		fileIDtoTblFile[tblFile.FileID()] = tblFile
		fileIds[i] = tblFile.FileID()
		fileIDtoNumChunks[tblFile.FileID()] = tblFile.NumChunks()
	}

	return fileIds, fileIDtoTblFile, fileIDtoNumChunks
}

const concurrentTableFileDownloads = 3

func clone(ctx context.Context, srcTS, sinkTS chunks.TableFileStore, sinkCS chunks.ChunkStore, getAddrs chunks.GetAddrsCurry, eventCh chan<- TableFileEvent) error {
	root, sourceFiles, appendixFiles, err := srcTS.Sources(ctx)
	if err != nil {
		return err
	}

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
		for i := 0; i < len(desiredFiles); i++ {
			if completed[i] {
				continue
			}
			if err := sem.Acquire(ctx, 1); err != nil {
				// The errgroup ctx has been canceled. We will
				// return the error from wg.Wait() below.
				break
			}
			idx := i
			eg.Go(func() (err error) {
				defer sem.Release(1)

				fileID := desiredFiles[idx]
				tblFile, ok := fileIDToTF[fileID]
				if !ok {
					// conjoin happened during clone
					return backoff.Permanent(errors.New("table file not found. please try again"))
				}

				report(TableFileEvent{EventType: DownloadStart, TableFiles: []chunks.TableFile{tblFile}})
				err = sinkTS.WriteTableFile(ctx, tblFile.FileID(), tblFile.NumChunks(), nil, func() (io.ReadCloser, uint64, error) {
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
				}

				report(TableFileEvent{EventType: DownloadSuccess, TableFiles: []chunks.TableFile{tblFile}})
				completed[idx] = true
				return nil
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
		if _, sourceFiles, appendixFiles, err = srcTS.Sources(ctx); err != nil {
			return err
		} else {
			tblFiles = filterAppendicesFromSourceFiles(appendixFiles, sourceFiles)
			_, fileIDToTF, _ = mapTableFiles(tblFiles)
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

	return sinkTS.SetRootChunk(ctx, root, hash.Hash{})
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
