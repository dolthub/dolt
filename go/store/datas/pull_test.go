// Copyright 2019 Liquidata, Inc.
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

package datas

import (
	"context"
	"github.com/liquidata-inc/dolt/go/store/d"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/types"
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
	sink        Database
	source      Database
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
	suite.sink = NewDatabase(suite.sinkCS)
	suite.source = NewDatabase(suite.sourceCS)
}

type RemoteToLocalSuite struct {
	PullSuite
}

func (suite *RemoteToLocalSuite) SetupTest() {
	suite.sinkCS, suite.sourceCS = makeTestStoreViews()
	suite.sink = NewDatabase(suite.sinkCS)
	suite.source = makeRemoteDb(suite.sourceCS)
}

type LocalToRemoteSuite struct {
	PullSuite
}

func (suite *LocalToRemoteSuite) SetupTest() {
	suite.sinkCS, suite.sourceCS = makeTestStoreViews()
	suite.sink = makeRemoteDb(suite.sinkCS)
	suite.source = NewDatabase(suite.sourceCS)
	suite.commitReads = 1
}

type RemoteToRemoteSuite struct {
	PullSuite
}

func (suite *RemoteToRemoteSuite) SetupTest() {
	suite.sinkCS, suite.sourceCS = makeTestStoreViews()
	suite.sink = makeRemoteDb(suite.sinkCS)
	suite.source = makeRemoteDb(suite.sourceCS)
	suite.commitReads = 1
}

func makeRemoteDb(cs chunks.ChunkStore) Database {
	return NewDatabase(cs)
}

func (suite *PullSuite) TearDownTest() {
	suite.sink.Close()
	suite.source.Close()
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
	expectedReads := suite.sinkCS.Reads

	l := buildListOfHeight(2, suite.source)
	sourceRef := suite.commitToSource(l, mustSet(types.NewSet(context.Background(), suite.source)))
	pt := startProgressTracker()

	err := Pull(context.Background(), suite.source, suite.sink, sourceRef, pt.Ch)
	suite.NoError(err)
	suite.True(expectedReads-suite.sinkCS.Reads <= suite.commitReads)
	pt.Validate(suite)

	v := mustValue(suite.sink.ReadValue(context.Background(), sourceRef.TargetHash())).(types.Struct)
	suite.NotNil(v)
	suite.True(l.Equals(mustGetValue(v.MaybeGet(ValueField))))
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
	sinkL := buildListOfHeight(2, suite.sink)
	suite.commitToSink(sinkL, mustSet(types.NewSet(context.Background(), suite.sink)))
	expectedReads := suite.sinkCS.Reads

	srcL := buildListOfHeight(2, suite.source)
	sourceRef := suite.commitToSource(srcL, mustSet(types.NewSet(context.Background(), suite.source)))
	srcL = buildListOfHeight(4, suite.source)
	sourceRef = suite.commitToSource(srcL, mustSet(types.NewSet(context.Background(), suite.source, sourceRef)))
	srcL = buildListOfHeight(5, suite.source)
	sourceRef = suite.commitToSource(srcL, mustSet(types.NewSet(context.Background(), suite.source, sourceRef)))

	pt := startProgressTracker()

	err := Pull(context.Background(), suite.source, suite.sink, sourceRef, pt.Ch)
	suite.NoError(err)

	suite.True(expectedReads-suite.sinkCS.Reads <= suite.commitReads)
	pt.Validate(suite)

	v, err := suite.sink.ReadValue(context.Background(), sourceRef.TargetHash())
	suite.NoError(err)
	suite.NotNil(v)
	suite.True(srcL.Equals(mustGetValue(v.(types.Struct).MaybeGet(ValueField))))
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
	sinkL := buildListOfHeight(3, suite.sink)
	sinkRef := suite.commitToSink(sinkL, mustSet(types.NewSet(context.Background(), suite.sink)))
	srcL := buildListOfHeight(3, suite.source)
	sourceRef := suite.commitToSource(srcL, mustSet(types.NewSet(context.Background(), suite.source)))

	var err error
	sinkL, err = sinkL.Edit().Append(types.String("oy!")).List(context.Background())
	suite.NoError(err)
	sinkRef = suite.commitToSink(sinkL, mustSet(types.NewSet(context.Background(), suite.sink, sinkRef)))
	srcL, err = srcL.Edit().Set(1, buildListOfHeight(5, suite.source)).List(context.Background())
	suite.NoError(err)
	sourceRef = suite.commitToSource(srcL, mustSet(types.NewSet(context.Background(), suite.source, sourceRef)))
	preReads := suite.sinkCS.Reads

	pt := startProgressTracker()

	err = Pull(context.Background(), suite.source, suite.sink, sourceRef, pt.Ch)
	suite.NoError(err)

	suite.True(preReads-suite.sinkCS.Reads <= suite.commitReads)
	pt.Validate(suite)

	v, err := suite.sink.ReadValue(context.Background(), sourceRef.TargetHash())
	suite.NoError(err)
	suite.NotNil(v)
	suite.True(srcL.Equals(mustGetValue(v.(types.Struct).MaybeGet(ValueField))))
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
	sinkL := buildListOfHeight(4, suite.sink)
	suite.commitToSink(sinkL, mustSet(types.NewSet(context.Background(), suite.sink)))
	expectedReads := suite.sinkCS.Reads

	srcL := buildListOfHeight(4, suite.source)
	sourceRef := suite.commitToSource(srcL, mustSet(types.NewSet(context.Background(), suite.source)))
	L3 := mustValue(mustValue(srcL.Get(context.Background(), 1)).(types.Ref).TargetValue(context.Background(), suite.source)).(types.List)
	L2 := mustValue(mustValue(L3.Get(context.Background(), 1)).(types.Ref).TargetValue(context.Background(), suite.source)).(types.List)
	L2Ed := L2.Edit().Append(mustRef(suite.source.WriteValue(context.Background(), types.String("oy!"))))
	L2, err := L2Ed.List(context.Background())
	suite.NoError(err)
	L3Ed := L3.Edit().Set(1, mustRef(suite.source.WriteValue(context.Background(), L2)))
	L3, err = L3Ed.List(context.Background())
	suite.NoError(err)
	srcLEd := srcL.Edit().Set(1, mustRef(suite.source.WriteValue(context.Background(), L3)))
	srcL, err = srcLEd.List(context.Background())
	suite.NoError(err)
	sourceRef = suite.commitToSource(srcL, mustSet(types.NewSet(context.Background(), suite.source, sourceRef)))

	pt := startProgressTracker()

	err = Pull(context.Background(), suite.source, suite.sink, sourceRef, pt.Ch)
	suite.NoError(err)

	suite.True(expectedReads-suite.sinkCS.Reads <= suite.commitReads)
	pt.Validate(suite)

	v, err := suite.sink.ReadValue(context.Background(), sourceRef.TargetHash())
	suite.NoError(err)
	suite.NotNil(v)
	suite.True(srcL.Equals(mustGetValue(v.(types.Struct).MaybeGet(ValueField))))
}

func (suite *PullSuite) commitToSource(v types.Value, p types.Set) types.Ref {
	ds, err := suite.source.GetDataset(context.Background(), datasetID)
	suite.NoError(err)
	ds, err = suite.source.Commit(context.Background(), ds, v, CommitOptions{Parents: p})
	suite.NoError(err)
	return mustHeadRef(ds)
}

func (suite *PullSuite) commitToSink(v types.Value, p types.Set) types.Ref {
	ds, err := suite.sink.GetDataset(context.Background(), datasetID)
	suite.NoError(err)
	ds, err = suite.sink.Commit(context.Background(), ds, v, CommitOptions{Parents: p})
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
