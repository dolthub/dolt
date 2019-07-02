// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"context"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/stretchr/testify/suite"
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
	sourceRef := suite.commitToSource(l, types.NewSet(context.Background(), suite.source))
	pt := startProgressTracker()

	Pull(context.Background(), suite.source, suite.sink, sourceRef, pt.Ch)
	suite.True(expectedReads-suite.sinkCS.Reads <= suite.commitReads)
	pt.Validate(suite)

	v := suite.sink.ReadValue(context.Background(), sourceRef.TargetHash()).(types.Struct)
	suite.NotNil(v)
	suite.True(l.Equals(types.Format_7_18, v.Get(ValueField)))
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
	suite.commitToSink(sinkL, types.NewSet(context.Background(), suite.sink))
	expectedReads := suite.sinkCS.Reads

	srcL := buildListOfHeight(2, suite.source)
	sourceRef := suite.commitToSource(srcL, types.NewSet(context.Background(), suite.source))
	srcL = buildListOfHeight(4, suite.source)
	sourceRef = suite.commitToSource(srcL, types.NewSet(context.Background(), suite.source, sourceRef))
	srcL = buildListOfHeight(5, suite.source)
	sourceRef = suite.commitToSource(srcL, types.NewSet(context.Background(), suite.source, sourceRef))

	pt := startProgressTracker()

	Pull(context.Background(), suite.source, suite.sink, sourceRef, pt.Ch)

	suite.True(expectedReads-suite.sinkCS.Reads <= suite.commitReads)
	pt.Validate(suite)

	v := suite.sink.ReadValue(context.Background(), sourceRef.TargetHash()).(types.Struct)
	suite.NotNil(v)
	suite.True(srcL.Equals(types.Format_7_18, v.Get(ValueField)))
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
	sinkRef := suite.commitToSink(sinkL, types.NewSet(context.Background(), suite.sink))
	srcL := buildListOfHeight(3, suite.source)
	sourceRef := suite.commitToSource(srcL, types.NewSet(context.Background(), suite.source))

	sinkL = sinkL.Edit().Append(types.String("oy!")).List(context.Background())
	sinkRef = suite.commitToSink(sinkL, types.NewSet(context.Background(), suite.sink, sinkRef))
	srcL = srcL.Edit().Set(1, buildListOfHeight(5, suite.source)).List(context.Background())
	sourceRef = suite.commitToSource(srcL, types.NewSet(context.Background(), suite.source, sourceRef))
	preReads := suite.sinkCS.Reads

	pt := startProgressTracker()

	Pull(context.Background(), suite.source, suite.sink, sourceRef, pt.Ch)

	suite.True(preReads-suite.sinkCS.Reads <= suite.commitReads)
	pt.Validate(suite)

	v := suite.sink.ReadValue(context.Background(), sourceRef.TargetHash()).(types.Struct)
	suite.NotNil(v)
	suite.True(srcL.Equals(types.Format_7_18, v.Get(ValueField)))
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
	suite.commitToSink(sinkL, types.NewSet(context.Background(), suite.sink))
	expectedReads := suite.sinkCS.Reads

	srcL := buildListOfHeight(4, suite.source)
	sourceRef := suite.commitToSource(srcL, types.NewSet(context.Background(), suite.source))
	L3 := srcL.Get(context.Background(), 1).(types.Ref).TargetValue(context.Background(), suite.source).(types.List)
	L2 := L3.Get(context.Background(), 1).(types.Ref).TargetValue(context.Background(), suite.source).(types.List)
	L2 = L2.Edit().Append(suite.source.WriteValue(context.Background(), types.String("oy!"))).List(context.Background())
	L3 = L3.Edit().Set(1, suite.source.WriteValue(context.Background(), L2)).List(context.Background())
	srcL = srcL.Edit().Set(1, suite.source.WriteValue(context.Background(), L3)).List(context.Background())
	sourceRef = suite.commitToSource(srcL, types.NewSet(context.Background(), suite.source, sourceRef))

	pt := startProgressTracker()

	Pull(context.Background(), suite.source, suite.sink, sourceRef, pt.Ch)

	suite.True(expectedReads-suite.sinkCS.Reads <= suite.commitReads)
	pt.Validate(suite)

	v := suite.sink.ReadValue(context.Background(), sourceRef.TargetHash()).(types.Struct)
	suite.NotNil(v)
	suite.True(srcL.Equals(types.Format_7_18, v.Get(ValueField)))
}

func (suite *PullSuite) commitToSource(v types.Value, p types.Set) types.Ref {
	ds := suite.source.GetDataset(context.Background(), datasetID)
	ds, err := suite.source.Commit(context.Background(), ds, v, CommitOptions{Parents: p})
	suite.NoError(err)
	return ds.HeadRef()
}

func (suite *PullSuite) commitToSink(v types.Value, p types.Set) types.Ref {
	ds := suite.sink.GetDataset(context.Background(), datasetID)
	ds, err := suite.sink.Commit(context.Background(), ds, v, CommitOptions{Parents: p})
	suite.NoError(err)
	return ds.HeadRef()
}

func buildListOfHeight(height int, vrw types.ValueReadWriter) types.List {
	unique := 0
	l := types.NewList(context.Background(), vrw, types.Float(unique), types.Float(unique+1))
	unique += 2

	for i := 0; i < height; i++ {
		r1, r2 := vrw.WriteValue(context.Background(), types.Float(unique)), vrw.WriteValue(context.Background(), l)
		unique++
		l = types.NewList(context.Background(), vrw, r1, r2)
	}
	return l
}
