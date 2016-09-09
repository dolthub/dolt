// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"os"
	"testing"

	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

type nomsCommitTestSuite struct {
	clienttest.ClientTestSuite
}

func TestNomsCommit(t *testing.T) {
	suite.Run(t, &nomsCommitTestSuite{})
}

func (s *nomsCommitTestSuite) setupDataset(name string, doCommit bool) (ds dataset.Dataset, dsStr string, ref types.Ref) {
	dsStr = spec.CreateValueSpecString("ldb", s.LdbDir, name)
	ds, err := spec.GetDataset(dsStr)
	s.NoError(err)

	v := types.String("testcommit")
	ref = ds.Database().WriteValue(v)
	if doCommit {
		ds, err = ds.CommitValue(v)
		s.NoError(err)
	}
	return
}

func (s *nomsCommitTestSuite) TestNomsCommitReadPathFromStdin() {
	ds, dsStr, ref := s.setupDataset("commitTestStdin", false)
	defer ds.Database().Close()

	_, ok := ds.MaybeHead()
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
	stdoutString, stderrString := s.MustRun(main, []string{"commit", dsStr})
	s.Empty(stderrString)
	s.Contains(stdoutString, "New head #")

	ds, err = spec.GetDataset(dsStr)
	s.NoError(err)
	commit, ok := ds.MaybeHead()
	s.True(ok, "should have a commit now")
	value := commit.Get(datas.ValueField)
	s.True(value.Hash() == ref.TargetHash(), "commit.value hash == writevalue hash")

	meta := commit.Get(datas.MetaField).(types.Struct)
	s.NotEmpty(meta.Get("date"))
}

func (s *nomsCommitTestSuite) TestNomsCommitToDatasetWithoutHead() {
	ds, dsStr, ref := s.setupDataset("commitTest", false)
	defer ds.Database().Close()

	_, ok := ds.MaybeHead()
	s.False(ok, "should not have a commit")

	stdoutString, stderrString := s.MustRun(main, []string{"commit", "#" + ref.TargetHash().String(), dsStr})
	s.Empty(stderrString)
	s.Contains(stdoutString, "New head #")

	ds, err := spec.GetDataset(dsStr)
	s.NoError(err)
	commit, ok := ds.MaybeHead()
	s.True(ok, "should have a commit now")
	value := commit.Get(datas.ValueField)
	s.True(value.Hash() == ref.TargetHash(), "commit.value hash == writevalue hash")

	meta := commit.Get(datas.MetaField).(types.Struct)
	s.NotEmpty(meta.Get("date"))
}

func structFieldEqual(old, now types.Struct, field string) bool {
	oldValue, oldOk := old.MaybeGet(field)
	nowValue, nowOk := now.MaybeGet(field)
	return oldOk && nowOk && nowValue.Equals(oldValue)
}

func (s *nomsCommitTestSuite) runDuplicateTest(allowDuplicate bool) {
	ds, dsStr, ref := s.setupDataset("commitTestDuplicate", true)
	defer ds.Database().Close()

	_, ok := ds.MaybeHeadValue()
	s.True(ok, "should have a commit")

	cliOptions := []string{"commit"}
	if allowDuplicate {
		cliOptions = append(cliOptions, "--allow-dupe=1")
	}
	cliOptions = append(cliOptions, "#"+ref.TargetHash().String(), dsStr)

	stdoutString, stderrString := s.MustRun(main, cliOptions)
	s.Empty(stderrString)
	if allowDuplicate {
		s.NotContains(stdoutString, "Commit aborted")
		s.Contains(stdoutString, "New head #")
	} else {
		s.Contains(stdoutString, "Commit aborted")
	}

	ds, err := spec.GetDataset(dsStr)
	s.NoError(err)
	value, ok := ds.MaybeHeadValue()
	s.True(ok, "should still have a commit")
	s.True(value.Hash() == ref.TargetHash(), "commit.value hash == previous commit hash")
}

func (s *nomsCommitTestSuite) TestNomsCommitDuplicate() {
	s.runDuplicateTest(false)
	s.runDuplicateTest(true)
}

func (s *nomsCommitTestSuite) TestNomsCommitMetadata() {
	ds, dsStr, ref := s.setupDataset("commitTestMetadata", true)
	metaOld := ds.Head().Get(datas.MetaField).(types.Struct)

	stdoutString, stderrString := s.MustRun(main, []string{"commit", "--allow-dupe=1", "--message=foo", "#" + ref.TargetHash().String(), dsStr})
	s.Empty(stderrString)
	s.Contains(stdoutString, "New head #")

	ds, err := spec.GetDataset(dsStr)
	s.NoError(err)
	metaNew := ds.Head().Get(datas.MetaField).(types.Struct)

	s.False(metaOld.Equals(metaNew), "meta didn't change")
	s.False(structFieldEqual(metaOld, metaNew, "date"), "date didn't change")
	s.False(structFieldEqual(metaOld, metaNew, "message"), "message didn't change")
	s.True(metaNew.Get("message").Equals(types.String("foo")), "message wasn't set")

	metaOld = metaNew
	stdoutString, stderrString = s.MustRun(main, []string{"commit", "--allow-dupe=1", "--meta=message=bar", "--date=" + spec.CommitMetaDateFormat, "#" + ref.TargetHash().String(), dsStr})
	s.Empty(stderrString)
	s.Contains(stdoutString, "New head #")

	ds, err = spec.GetDataset(dsStr)
	s.NoError(err)
	metaNew = ds.Head().Get(datas.MetaField).(types.Struct)
	s.False(metaOld.Equals(metaNew), "meta didn't change")
	s.False(structFieldEqual(metaOld, metaNew, "date"), "date didn't change")
	s.False(structFieldEqual(metaOld, metaNew, "message"), "message didn't change")
	s.True(metaNew.Get("message").Equals(types.String("bar")), "message wasn't set")
}

func (s *nomsCommitTestSuite) TestNomsCommitHashNotFound() {
	s.Panics(func() {
		s.MustRun(main, []string{"commit", "#9ei6fbrs0ujo51vifd3f2eebufo4lgdu", "f::b"})
	})
}

func (s *nomsCommitTestSuite) TestNomsCommitMetadataBadDateFormat() {
	ds, dsStr, ref := s.setupDataset("commitTestMetadata", true)
	defer ds.Database().Close()

	s.Panics(func() {
		s.MustRun(main, []string{"commit", "--allow-dupe=1", "--date=a", "#" + ref.TargetHash().String(), dsStr})
	})
}

func (s *nomsCommitTestSuite) TestNomsCommitInvalidMetadataPaths() {
	ds, dsStr, ref := s.setupDataset("commitTestMetadataPaths", true)
	defer ds.Database().Close()

	s.Panics(func() {
		s.MustRun(main, []string{"commit", "--allow-dupe=1", "--meta-p=#beef", "#" + ref.TargetHash().String(), dsStr})
	})
}

func (s *nomsCommitTestSuite) TestNomsCommitInvalidMetadataFieldName() {
	ds, dsStr, ref := s.setupDataset("commitTestMetadataFields", true)
	defer ds.Database().Close()

	s.Panics(func() {
		s.MustRun(main, []string{"commit", "--allow-dupe=1", "--meta=_foo=bar", "#" + ref.TargetHash().String(), dsStr})
	})
}
