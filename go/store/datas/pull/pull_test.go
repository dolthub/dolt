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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package pull

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

const datasetID = "ds1"

func TestLocalToLocalPulls(t *testing.T) {
	suite.Run(t, &LocalToLocalSuite{})
}

func TestRemoteToLocalPulls(t *testing.T) {
	suite.Run(t, &RemoteToLocalSuite{})
}

func TestLocalToRemotePulls(t *testing.T) {
	suite.Run(t, &LocalToRemoteSuite{})
}

func TestRemoteToRemotePulls(t *testing.T) {
	suite.Run(t, &RemoteToRemoteSuite{})
}

type PullSuite struct {
	suite.Suite
	sinkCS      *chunks.TestStoreView
	sourceCS    *chunks.TestStoreView
	sinkVRW     types.ValueReadWriter
	sourceVRW   types.ValueReadWriter
	commitReads int // The number of reads triggered by commit differs across chunk store impls
}

func makeTestStoreViews() (ts1, ts2 *chunks.TestStoreView) {
	st1, st2 := &chunks.TestStorage{}, &chunks.TestStorage{}
	return st1.NewView(), st2.NewView()
}

type LocalToLocalSuite struct {
	PullSuite
}

func (suite *LocalToLocalSuite) SetupTest() {
	suite.sinkCS, suite.sourceCS = makeTestStoreViews()
	suite.sinkVRW, suite.sourceVRW = types.NewValueStore(suite.sinkCS), types.NewValueStore(suite.sourceCS)
}

type RemoteToLocalSuite struct {
	PullSuite
}

func (suite *RemoteToLocalSuite) SetupTest() {
	suite.sinkCS, suite.sourceCS = makeTestStoreViews()
	suite.sinkVRW, suite.sourceVRW = types.NewValueStore(suite.sinkCS), types.NewValueStore(suite.sourceCS)
}

type LocalToRemoteSuite struct {
	PullSuite
}

func (suite *LocalToRemoteSuite) SetupTest() {
	suite.sinkCS, suite.sourceCS = makeTestStoreViews()
	suite.sinkVRW, suite.sourceVRW = types.NewValueStore(suite.sinkCS), types.NewValueStore(suite.sourceCS)
	suite.commitReads = 1
}

type RemoteToRemoteSuite struct {
	PullSuite
}

func (suite *RemoteToRemoteSuite) SetupTest() {
	suite.sinkCS, suite.sourceCS = makeTestStoreViews()
	suite.sinkVRW, suite.sourceVRW = types.NewValueStore(suite.sinkCS), types.NewValueStore(suite.sourceCS)
	suite.commitReads = 1
}

func (suite *PullSuite) TearDownTest() {
	suite.sinkCS.Close()
	suite.sourceCS.Close()
}

type progressTracker struct {
	Ch     chan PullProgress
	doneCh chan []PullProgress
}

func startProgressTracker() *progressTracker {
	pt := &progressTracker{make(chan PullProgress), make(chan []PullProgress)}
	go func() {
		progress := []PullProgress{}
		for info := range pt.Ch {
			progress = append(progress, info)
		}
		pt.doneCh <- progress
	}()
	return pt
}

func (pt *progressTracker) Validate(suite *PullSuite) {
	close(pt.Ch)
	progress := <-pt.doneCh

	// Expecting exact progress would be unreliable and not necessary meaningful. Instead, just validate that it's useful and consistent.
	suite.NotEmpty(progress)

	first := progress[0]
	suite.Zero(first.DoneCount)
	suite.True(first.KnownCount > 0)
	suite.Zero(first.ApproxWrittenBytes)

	last := progress[len(progress)-1]
	suite.True(last.DoneCount > 0)
	suite.Equal(last.DoneCount, last.KnownCount)

	for i, prog := range progress {
		suite.True(prog.KnownCount >= prog.DoneCount)
		if i > 0 {
			prev := progress[i-1]
			suite.True(prog.DoneCount >= prev.DoneCount)
			suite.True(prog.ApproxWrittenBytes >= prev.ApproxWrittenBytes)
		}
	}
}

// Source: -3-> C(L2) -1-> N
//                 \  -2-> L1 -1-> N
//                          \ -1-> L0
//
// Sink: Nada
func (suite *PullSuite) TestPullEverything() {
	expectedReads := suite.sinkCS.Reads()

	l := buildListOfHeight(2, suite.sourceVRW)
	sourceRef := suite.commitToSource(l, mustList(types.NewList(context.Background(), suite.sourceVRW)))
	pt := startProgressTracker()

	wrf, err := types.WalkRefsForChunkStore(suite.sourceCS)
	suite.NoError(err)
	err = Pull(context.Background(), suite.sourceCS, suite.sinkCS, wrf, sourceRef.TargetHash(), pt.Ch)
	suite.NoError(err)
	suite.True(expectedReads-suite.sinkCS.Reads() <= suite.commitReads)
	pt.Validate(suite)

	v := mustValue(suite.sinkVRW.ReadValue(context.Background(), sourceRef.TargetHash())).(types.Struct)
	suite.NotNil(v)
	suite.True(l.Equals(mustGetValue(v.MaybeGet(datas.ValueField))))
}

// Source: -6-> C3(L5) -1-> N
//               .  \  -5-> L4 -1-> N
//                .          \ -4-> L3 -1-> N
//                 .                 \  -3-> L2 -1-> N
//                  5                         \ -2-> L1 -1-> N
//                   .                                \ -1-> L0
//                  C2(L4) -1-> N
//                   .  \  -4-> L3 -1-> N
//                    .          \ -3-> L2 -1-> N
//                     .                 \ -2-> L1 -1-> N
//                      3                        \ -1-> L0
//                       .
//                     C1(L2) -1-> N
//                         \  -2-> L1 -1-> N
//                                  \ -1-> L0
//
// Sink: -3-> C1(L2) -1-> N
//                \  -2-> L1 -1-> N
//                         \ -1-> L0
func (suite *PullSuite) TestPullMultiGeneration() {
	sinkL := buildListOfHeight(2, suite.sinkVRW)
	suite.commitToSink(sinkL, mustList(types.NewList(context.Background(), suite.sinkVRW)))
	expectedReads := suite.sinkCS.Reads()

	srcL := buildListOfHeight(2, suite.sourceVRW)
	sourceRef := suite.commitToSource(srcL, mustList(types.NewList(context.Background(), suite.sourceVRW)))
	srcL = buildListOfHeight(4, suite.sourceVRW)
	sourceRef = suite.commitToSource(srcL, mustList(types.NewList(context.Background(), suite.sourceVRW, sourceRef)))
	srcL = buildListOfHeight(5, suite.sourceVRW)
	sourceRef = suite.commitToSource(srcL, mustList(types.NewList(context.Background(), suite.sourceVRW, sourceRef)))

	pt := startProgressTracker()

	wrf, err := types.WalkRefsForChunkStore(suite.sourceCS)
	suite.NoError(err)
	err = Pull(context.Background(), suite.sourceCS, suite.sinkCS, wrf, sourceRef.TargetHash(), pt.Ch)
	suite.NoError(err)

	suite.True(expectedReads-suite.sinkCS.Reads() <= suite.commitReads)
	pt.Validate(suite)

	v, err := suite.sinkVRW.ReadValue(context.Background(), sourceRef.TargetHash())
	suite.NoError(err)
	suite.NotNil(v)
	suite.True(srcL.Equals(mustGetValue(v.(types.Struct).MaybeGet(datas.ValueField))))
}

// Source: -6-> C2(L5) -1-> N
//               .  \  -5-> L4 -1-> N
//                .          \ -4-> L3 -1-> N
//                 .                 \  -3-> L2 -1-> N
//                  4                         \ -2-> L1 -1-> N
//                   .                                \ -1-> L0
//                  C1(L3) -1-> N
//                      \  -3-> L2 -1-> N
//                               \ -2-> L1 -1-> N
//                                       \ -1-> L0
//
// Sink: -5-> C3(L3') -1-> N
//             .   \ -3-> L2 -1-> N
//              .   \      \ -2-> L1 -1-> N
//               .   \             \ -1-> L0
//                .   \  - "oy!"
//                 4
//                  .
//                C1(L3) -1-> N
//                    \  -3-> L2 -1-> N
//                             \ -2-> L1 -1-> N
//                                     \ -1-> L0
func (suite *PullSuite) TestPullDivergentHistory() {
	sinkL := buildListOfHeight(3, suite.sinkVRW)
	sinkRef := suite.commitToSink(sinkL, mustList(types.NewList(context.Background(), suite.sinkVRW)))
	srcL := buildListOfHeight(3, suite.sourceVRW)
	sourceRef := suite.commitToSource(srcL, mustList(types.NewList(context.Background(), suite.sourceVRW)))

	var err error
	sinkL, err = sinkL.Edit().Append(types.String("oy!")).List(context.Background())
	suite.NoError(err)
	sinkRef = suite.commitToSink(sinkL, mustList(types.NewList(context.Background(), suite.sinkVRW, sinkRef)))
	srcL, err = srcL.Edit().Set(1, buildListOfHeight(5, suite.sourceVRW)).List(context.Background())
	suite.NoError(err)
	sourceRef = suite.commitToSource(srcL, mustList(types.NewList(context.Background(), suite.sourceVRW, sourceRef)))
	preReads := suite.sinkCS.Reads()

	pt := startProgressTracker()

	wrf, err := types.WalkRefsForChunkStore(suite.sourceCS)
	suite.NoError(err)
	err = Pull(context.Background(), suite.sourceCS, suite.sinkCS, wrf, sourceRef.TargetHash(), pt.Ch)
	suite.NoError(err)

	suite.True(preReads-suite.sinkCS.Reads() <= suite.commitReads)
	pt.Validate(suite)

	v, err := suite.sinkVRW.ReadValue(context.Background(), sourceRef.TargetHash())
	suite.NoError(err)
	suite.NotNil(v)
	suite.True(srcL.Equals(mustGetValue(v.(types.Struct).MaybeGet(datas.ValueField))))
}

// Source: -6-> C2(L4) -1-> N
//               .  \  -4-> L3 -1-> N
//                 .         \ -3-> L2 -1-> N
//                  .                \ - "oy!"
//                   5                \ -2-> L1 -1-> N
//                    .                       \ -1-> L0
//                   C1(L4) -1-> N
//                       \  -4-> L3 -1-> N
//                                \ -3-> L2 -1-> N
//                                        \ -2-> L1 -1-> N
//                                                \ -1-> L0
// Sink: -5-> C1(L4) -1-> N
//                \  -4-> L3 -1-> N
//                         \ -3-> L2 -1-> N
//                                 \ -2-> L1 -1-> N
//                                         \ -1-> L0
func (suite *PullSuite) TestPullUpdates() {
	sinkL := buildListOfHeight(4, suite.sinkVRW)
	suite.commitToSink(sinkL, mustList(types.NewList(context.Background(), suite.sinkVRW)))
	expectedReads := suite.sinkCS.Reads()

	srcL := buildListOfHeight(4, suite.sourceVRW)
	sourceRef := suite.commitToSource(srcL, mustList(types.NewList(context.Background(), suite.sourceVRW)))
	L3 := mustValue(mustValue(srcL.Get(context.Background(), 1)).(types.Ref).TargetValue(context.Background(), suite.sourceVRW)).(types.List)
	L2 := mustValue(mustValue(L3.Get(context.Background(), 1)).(types.Ref).TargetValue(context.Background(), suite.sourceVRW)).(types.List)
	L2Ed := L2.Edit().Append(mustRef(suite.sourceVRW.WriteValue(context.Background(), types.String("oy!"))))
	L2, err := L2Ed.List(context.Background())
	suite.NoError(err)
	L3Ed := L3.Edit().Set(1, mustRef(suite.sourceVRW.WriteValue(context.Background(), L2)))
	L3, err = L3Ed.List(context.Background())
	suite.NoError(err)
	srcLEd := srcL.Edit().Set(1, mustRef(suite.sourceVRW.WriteValue(context.Background(), L3)))
	srcL, err = srcLEd.List(context.Background())
	suite.NoError(err)
	sourceRef = suite.commitToSource(srcL, mustList(types.NewList(context.Background(), suite.sourceVRW, sourceRef)))

	pt := startProgressTracker()

	wrf, err := types.WalkRefsForChunkStore(suite.sourceCS)
	suite.NoError(err)
	err = Pull(context.Background(), suite.sourceCS, suite.sinkCS, wrf, sourceRef.TargetHash(), pt.Ch)
	suite.NoError(err)

	suite.True(expectedReads-suite.sinkCS.Reads() <= suite.commitReads)
	pt.Validate(suite)

	v, err := suite.sinkVRW.ReadValue(context.Background(), sourceRef.TargetHash())
	suite.NoError(err)
	suite.NotNil(v)
	suite.True(srcL.Equals(mustGetValue(v.(types.Struct).MaybeGet(datas.ValueField))))
}

func (suite *PullSuite) commitToSource(v types.Value, p types.List) types.Ref {
	db := datas.NewTypesDatabase(suite.sourceVRW.(*types.ValueStore))
	ds, err := db.GetDataset(context.Background(), datasetID)
	suite.NoError(err)
	ds, err = db.Commit(context.Background(), ds, v, datas.CommitOptions{ParentsList: p})
	suite.NoError(err)
	return mustHeadRef(ds)
}

func (suite *PullSuite) commitToSink(v types.Value, p types.List) types.Ref {
	db := datas.NewTypesDatabase(suite.sinkVRW.(*types.ValueStore))
	ds, err := db.GetDataset(context.Background(), datasetID)
	suite.NoError(err)
	ds, err = db.Commit(context.Background(), ds, v, datas.CommitOptions{ParentsList: p})
	suite.NoError(err)
	return mustHeadRef(ds)
}

func buildListOfHeight(height int, vrw types.ValueReadWriter) types.List {
	unique := 0
	l, err := types.NewList(context.Background(), vrw, types.Float(unique), types.Float(unique+1))
	d.PanicIfError(err)
	unique += 2

	for i := 0; i < height; i++ {
		r1, err := vrw.WriteValue(context.Background(), types.Float(unique))
		d.PanicIfError(err)
		r2, err := vrw.WriteValue(context.Background(), l)
		d.PanicIfError(err)
		unique++
		l, err = types.NewList(context.Background(), vrw, r1, r2)
		d.PanicIfError(err)
	}
	return l
}

type TestFailingTableFile struct {
	fileID    string
	numChunks int
}

func (ttf *TestFailingTableFile) FileID() string {
	return ttf.fileID
}

func (ttf *TestFailingTableFile) NumChunks() int {
	return ttf.numChunks
}

func (ttf *TestFailingTableFile) Open(ctx context.Context) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader([]byte{0x00})), errors.New("this is a test error")
}

type TestTableFile struct {
	fileID    string
	numChunks int
	data      []byte
}

func (ttf *TestTableFile) FileID() string {
	return ttf.fileID
}

func (ttf *TestTableFile) NumChunks() int {
	return ttf.numChunks
}

func (ttf *TestTableFile) Open(ctx context.Context) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(ttf.data)), nil
}

type TestTableFileWriter struct {
	fileID    string
	numChunks int
	writer    *bytes.Buffer
	ttfs      *TestTableFileStore
}

func (ttfWr *TestTableFileWriter) Write(data []byte) (int, error) {
	return ttfWr.writer.Write(data)
}

func (ttfWr *TestTableFileWriter) Close(ctx context.Context) error {
	data := ttfWr.writer.Bytes()
	ttfWr.writer = nil

	ttfWr.ttfs.mu.Lock()
	defer ttfWr.ttfs.mu.Unlock()
	ttfWr.ttfs.tableFiles[ttfWr.fileID] = &TestTableFile{ttfWr.fileID, ttfWr.numChunks, data}
	return nil
}

type TestTableFileStore struct {
	root       hash.Hash
	tableFiles map[string]*TestTableFile
	mu         sync.Mutex
}

var _ nbs.TableFileStore = &TestTableFileStore{}

func (ttfs *TestTableFileStore) Sources(ctx context.Context) (hash.Hash, []nbs.TableFile, []nbs.TableFile, error) {
	ttfs.mu.Lock()
	defer ttfs.mu.Unlock()
	var tblFiles []nbs.TableFile
	for _, tblFile := range ttfs.tableFiles {
		tblFiles = append(tblFiles, tblFile)
	}

	return ttfs.root, tblFiles, []nbs.TableFile{}, nil
}

func (ttfs *TestTableFileStore) Size(ctx context.Context) (uint64, error) {
	ttfs.mu.Lock()
	defer ttfs.mu.Unlock()
	sz := uint64(0)
	for _, tblFile := range ttfs.tableFiles {
		sz += uint64(len(tblFile.data))
	}
	return sz, nil
}

func (ttfs *TestTableFileStore) WriteTableFile(ctx context.Context, fileId string, numChunks int, rd io.Reader, contentLength uint64, contentHash []byte) error {
	tblFile := &TestTableFileWriter{fileId, numChunks, bytes.NewBuffer(nil), ttfs}
	_, err := io.Copy(tblFile, rd)

	if err != nil {
		return err
	}

	return tblFile.Close(ctx)
}

// AddTableFilesToManifest adds table files to the manifest
func (ttfs *TestTableFileStore) AddTableFilesToManifest(ctx context.Context, fileIdToNumChunks map[string]int) error {
	return nil
}

func (ttfs *TestTableFileStore) SetRootChunk(ctx context.Context, root, previous hash.Hash) error {
	ttfs.root = root
	return nil
}

type FlakeyTestTableFileStore struct {
	*TestTableFileStore
	GoodNow bool
}

func (f *FlakeyTestTableFileStore) Sources(ctx context.Context) (hash.Hash, []nbs.TableFile, []nbs.TableFile, error) {
	if !f.GoodNow {
		f.GoodNow = true
		r, files, appendixFiles, _ := f.TestTableFileStore.Sources(ctx)
		for i := range files {
			files[i] = &TestFailingTableFile{files[i].FileID(), files[i].NumChunks()}
		}
		return r, files, appendixFiles, nil
	}
	return f.TestTableFileStore.Sources(ctx)
}

func (ttfs *TestTableFileStore) SupportedOperations() nbs.TableFileStoreOps {
	return nbs.TableFileStoreOps{
		CanRead:  true,
		CanWrite: true,
	}
}

func (ttfs *TestTableFileStore) PruneTableFiles(ctx context.Context) error {
	return chunks.ErrUnsupportedOperation
}

func TestClone(t *testing.T) {
	hashBytes := [hash.ByteLen]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13}
	src := &TestTableFileStore{
		root: hash.Of(hashBytes[:]),
		tableFiles: map[string]*TestTableFile{
			"file1": &TestTableFile{
				fileID:    "file1",
				numChunks: 1,
				data:      []byte("Call me Ishmael. Some years ago—never mind how long precisely—having little or no money in my purse, "),
			},
			"file2": &TestTableFile{
				fileID:    "file2",
				numChunks: 2,
				data:      []byte("and nothing particular to interest me on shore, I thought I would sail about a little and see the watery "),
			},
			"file3": &TestTableFile{
				fileID:    "file3",
				numChunks: 3,
				data:      []byte("part of the world. It is a way I have of driving off the spleen and regulating the "),
			},
			"file4": &TestTableFile{
				fileID:    "file4",
				numChunks: 4,
				data:      []byte("circulation. Whenever I find myself growing grim about the mouth; whenever it is a damp, drizzly "),
			},
			"file5": &TestTableFile{
				fileID:    "file5",
				numChunks: 5,
				data:      []byte("November in my soul; whenever I find myself involuntarily pausing before coffin warehouses, and bringing "),
			},
		},
	}

	dest := &TestTableFileStore{
		root:       hash.Hash{},
		tableFiles: map[string]*TestTableFile{},
	}

	ctx := context.Background()
	err := clone(ctx, src, dest, nil)
	require.NoError(t, err)

	err = dest.SetRootChunk(ctx, src.root, hash.Hash{})
	require.NoError(t, err)

	assert.True(t, reflect.DeepEqual(src, dest))

	t.Run("WithFlakeyTableFileStore", func(t *testing.T) {
		// After a Clone()'s TableFile.Open() or a Read from the TableFile
		// fails, we retry with newly fetched Sources().
		flakeySrc := &FlakeyTestTableFileStore{
			TestTableFileStore: src,
		}

		dest = &TestTableFileStore{
			root:       hash.Hash{},
			tableFiles: map[string]*TestTableFile{},
		}

		err := clone(ctx, flakeySrc, dest, nil)
		require.NoError(t, err)

		err = dest.SetRootChunk(ctx, flakeySrc.root, hash.Hash{})
		require.NoError(t, err)

		assert.True(t, reflect.DeepEqual(flakeySrc.TestTableFileStore, dest))
	})
}

func mustList(l types.List, err error) types.List {
	d.PanicIfError(err)
	return l
}

func mustValue(val types.Value, err error) types.Value {
	d.PanicIfError(err)
	return val
}

func mustGetValue(v types.Value, found bool, err error) types.Value {
	d.PanicIfError(err)
	d.PanicIfFalse(found)
	return v
}

func mustRef(ref types.Ref, err error) types.Ref {
	d.PanicIfError(err)
	return ref
}

func mustHeadRef(ds datas.Dataset) types.Ref {
	hr, ok, err := ds.MaybeHeadRef()

	if err != nil {
		panic("error getting head")
	}

	if !ok {
		panic("no head")
	}

	return hr
}
