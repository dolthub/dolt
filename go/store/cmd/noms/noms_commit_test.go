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

package main

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/spec"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/clienttest"
)

type nomsCommitTestSuite struct {
	clienttest.ClientTestSuite
}

func TestNomsCommit(t *testing.T) {
	suite.Run(t, &nomsCommitTestSuite{})
}

func (s *nomsCommitTestSuite) setupDataset(name string, doCommit bool) (sp spec.Spec, ref types.Ref) {
	var err error
	sp, err = spec.ForDataset(spec.CreateValueSpecString("nbs", s.DBDir, name))
	s.NoError(err)

	v := types.String("testcommit")
	ctx := context.Background()
	vrw := sp.GetVRW(ctx)
	ref, err = vrw.WriteValue(context.Background(), v)
	s.NoError(err)

	if doCommit {
		db := sp.GetDatabase(context.Background())
		_, err = datas.CommitValue(context.Background(), db, sp.GetDataset(context.Background()), ref)
		s.NoError(err)
	}
	return
}

func (s *nomsCommitTestSuite) TestNomsCommitReadPathFromStdin() {
	sp, ref := s.setupDataset("commitTestStdin", false)
	defer sp.Close()

	_, ok := sp.GetDataset(context.Background()).MaybeHead()
	s.False(ok, "should not have a commit")

	oldStdin := os.Stdin
	newStdin, stdinWriter, err := os.Pipe()
	s.NoError(err)

	os.Stdin = newStdin
	defer func() {
		os.Stdin = oldStdin
	}()

	go func() {
		stdinWriter.Write([]byte("#" + ref.TargetHash().String() + "\n"))
		stdinWriter.Close()
	}()
	stdoutString, stderrString := s.MustRun(main, []string{"commit", sp.String()})
	s.Empty(stderrString)
	s.Contains(stdoutString, "New head #")

	sp, _ = spec.ForDataset(sp.String())
	defer sp.Close()

	commit, ok := sp.GetDataset(context.Background()).MaybeHead()
	s.True(ok, "should have a commit now")
	value, ok, err := commit.MaybeGet(datas.ValueField)
	s.NoError(err)
	s.True(ok)
	h, err := value.Hash(types.Format_7_18)
	s.NoError(err)
	s.True(h == ref.TargetHash(), "commit.value hash == writevalue hash")

	meta, ok, err := commit.MaybeGet(datas.CommitMetaField)
	s.NoError(err)
	s.True(ok)
	d, ok, err := meta.(types.Struct).MaybeGet("date")
	s.NoError(err)
	s.True(ok)
	s.NotEmpty(d)
}

func (s *nomsCommitTestSuite) TestNomsCommitToDatasetWithoutHead() {
	sp, ref := s.setupDataset("commitTest", false)
	defer sp.Close()

	_, ok := sp.GetDataset(context.Background()).MaybeHead()
	s.False(ok, "should not have a commit")

	stdoutString, stderrString := s.MustRun(main, []string{"commit", "#" + ref.TargetHash().String(), sp.String()})
	s.Empty(stderrString)
	s.Contains(stdoutString, "New head #")

	sp, _ = spec.ForDataset(sp.String())
	defer sp.Close()

	commit, ok := sp.GetDataset(context.Background()).MaybeHead()
	s.True(ok, "should have a commit now")
	value, ok, err := commit.MaybeGet(datas.ValueField)
	s.NoError(err)
	s.True(ok)
	h, err := value.Hash(types.Format_7_18)
	s.NoError(err)
	s.True(h == ref.TargetHash(), "commit.value hash == writevalue hash")

	metaVal, ok, err := commit.MaybeGet(datas.CommitMetaField)
	s.NoError(err)
	s.True(ok)
	meta := metaVal.(types.Struct)
	d, ok, err := meta.MaybeGet("date")
	s.NoError(err)
	s.True(ok)
	s.NotEmpty(d)
}

func structFieldEqual(old, now types.Struct, field string) bool {
	oldValue, oldOk, err := old.MaybeGet(field)
	d.PanicIfError(err)
	nowValue, nowOk, err := now.MaybeGet(field)
	d.PanicIfError(err)
	return oldOk && nowOk && nowValue.Equals(oldValue)
}

func (s *nomsCommitTestSuite) runDuplicateTest(allowDuplicate bool) {
	dsName := "commitTestDuplicate"
	sp, ref := s.setupDataset(dsName, true)
	defer sp.Close()

	_, ok, err := sp.GetDataset(context.Background()).MaybeHeadValue()
	s.NoError(err)
	s.True(ok, "should have a commit")

	cliOptions := []string{"commit"}
	if allowDuplicate {
		cliOptions = append(cliOptions, "--allow-dupe=1")
	}
	cliOptions = append(cliOptions, dsName+".value", sp.String())

	stdoutString, stderrString, errI := s.Run(main, cliOptions)
	s.Nil(errI)
	s.Empty(stderrString)
	if allowDuplicate {
		s.NotContains(stdoutString, "Commit aborted")
		s.Contains(stdoutString, "New head #")
	} else {
		s.Contains(stdoutString, "Commit aborted")
	}

	sp, _ = spec.ForDataset(sp.String())
	defer sp.Close()

	value, ok, err := sp.GetDataset(context.Background()).MaybeHeadValue()
	s.NoError(err)
	s.True(ok, "should still have a commit")
	valH, err := value.Hash(types.Format_7_18)
	s.NoError(err)
	refH, err := ref.Hash(types.Format_7_18)
	s.NoError(err)
	s.True(valH == refH, "commit.value hash == previous commit hash")
}

func (s *nomsCommitTestSuite) TestNomsCommitDuplicate() {
	s.runDuplicateTest(false)
	s.runDuplicateTest(true)
}

func (s *nomsCommitTestSuite) TestNomsCommitMetadata() {
	dsName := "commitTestMetadata"
	sp, _ := s.setupDataset(dsName, true)
	defer sp.Close()

	dsHead, ok := sp.GetDataset(context.Background()).MaybeHead()
	s.True(ok)
	metaOldVal, ok, err := dsHead.MaybeGet(datas.CommitMetaField)
	s.NoError(err)
	s.True(ok)
	metaOld := metaOldVal.(types.Struct)

	stdoutString, stderrString, errI := s.Run(main, []string{"commit", "--allow-dupe=1", "--message=foo", dsName + ".value", sp.String()})
	s.Nil(errI)
	s.Empty(stderrString)
	s.Contains(stdoutString, "New head #")

	sp, _ = spec.ForDataset(sp.String())
	defer sp.Close()

	dsHead, ok = sp.GetDataset(context.Background()).MaybeHead()
	s.True(ok)
	metaNewVal, ok, err := dsHead.MaybeGet(datas.CommitMetaField)
	s.NoError(err)
	s.True(ok)
	metaNew := metaNewVal.(types.Struct)

	s.False(metaOld.Equals(metaNew), "meta didn't change")
	s.False(structFieldEqual(metaOld, metaNew, "date"), "date didn't change")
	s.False(structFieldEqual(metaOld, metaNew, "message"), "message didn't change")
	msgVal, ok, err := metaNew.MaybeGet("message")
	s.NoError(err)
	s.True(ok)
	s.True(msgVal.Equals(types.String("foo")), "message wasn't set")

	metaOld = metaNew

	stdoutString, stderrString = s.MustRun(main, []string{"commit", "--allow-dupe=1", "--meta=message=bar", "--date=" + spec.CommitMetaDateFormat[:20], dsName + ".value", sp.String()})
	s.Empty(stderrString)
	s.Contains(stdoutString, "New head #")

	sp, _ = spec.ForDataset(sp.String())
	defer sp.Close()

	dsHead, ok = sp.GetDataset(context.Background()).MaybeHead()
	s.True(ok)
	metaNewVal, ok, err = dsHead.MaybeGet(datas.CommitMetaField)
	s.NoError(err)
	s.True(ok)
	metaNew = metaNewVal.(types.Struct)

	s.False(metaOld.Equals(metaNew), "meta didn't change")
	s.False(structFieldEqual(metaOld, metaNew, "date"), "date didn't change")
	s.False(structFieldEqual(metaOld, metaNew, "message"), "message didn't change")

	msgVal, ok, err = metaNew.MaybeGet("message")
	s.NoError(err)
	s.True(ok)
	s.True(msgVal.Equals(types.String("bar")), "message wasn't set")
}

func (s *nomsCommitTestSuite) TestNomsCommitHashNotFound() {
	sp, _ := s.setupDataset("commitTestBadHash", true)
	defer sp.Close()

	s.Panics(func() {
		s.MustRun(main, []string{"commit", "#9ei6fbrs0ujo51vifd3f2eebufo4lgdu", sp.String()})
	})
}

func (s *nomsCommitTestSuite) TestNomsCommitMetadataBadDateFormat() {
	sp, ref := s.setupDataset("commitTestMetadata", true)
	defer sp.Close()

	s.Panics(func() {
		s.MustRun(main, []string{"commit", "--allow-dupe=1", "--date=a", "#" + ref.TargetHash().String(), sp.String()})
	})
}

func (s *nomsCommitTestSuite) TestNomsCommitInvalidMetadataPaths() {
	sp, ref := s.setupDataset("commitTestMetadataPaths", true)
	defer sp.Close()

	s.Panics(func() {
		s.MustRun(main, []string{"commit", "--allow-dupe=1", "--meta-p=#beef", "#" + ref.TargetHash().String(), sp.String()})
	})
}

func (s *nomsCommitTestSuite) TestNomsCommitInvalidMetadataFieldName() {
	sp, ref := s.setupDataset("commitTestMetadataFields", true)
	defer sp.Close()

	s.Panics(func() {
		s.MustRun(main, []string{"commit", "--allow-dupe=1", "--meta=_foo=bar", "#" + ref.TargetHash().String(), sp.String()})
	})
}
