// Copyright 2022 Dolthub, Inc.
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

package nbs

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

const tempJournalPrefix = "nbs_journal_"

// rotateJournalFile swaps a full journal file for a new empty journal.
// It writes the previous journal to a new table file, updates the manifest,
//
//		and returns a new journalWriter and the contents of the new manifest.
//
//			The protocol for swapping journal files is as follows:
//	  1. Read the current manifest and assert it matches the contents
//			currently held in memory, with the exception of the root hash.
//		 2. Write out a new table file with the contents of the current
//		    journal file then fsync() the file and the NBS dir.
//		 3. In a tmp dir, instantiate a new pre-allocated journal file,
//		    write the current manifest root, and fsync() the file.
//		 4. Update the manifest with the table file from 2, this involves
//		    an fsync() for the new manifest, and for the NBS dir.
//		 5. Clobber the existing journal file, with the new journal from 3.
func rotateJournalFile(ctx context.Context, nbsPath string, m manifest, mc manifestContents) (manifestContents, *journalWriter, error) {
	var err error
	nbsPath, err = filepath.Abs(nbsPath)
	if err != nil {
		return manifestContents{}, nil, err
	}

	eg, ectx := errgroup.WithContext(ctx)
	eg.Go(func() error { // step 1: validate manifest
		return validatePersistedManifest(ectx, m, mc)
	})

	var spec tableSpec
	eg.Go(func() (err error) { // step 2: convert current journal to table
		spec, err = convertJournalToTable(ectx, nbsPath)
		return
	})

	var wr *journalWriter
	var tmpJournal string
	eg.Go(func() (err error) { // step 3: pre-allocate a new journal
		dir := tempfiles.MovableTempFileProvider.GetTempDir()
		tmpJournal = filepath.Join(dir, tempJournalPrefix+uuid.New().String())
		wr, err = createJournalWriter(ectx, tmpJournal)
		if err != nil {
			return err
		}
		if err = wr.WriteRootHash(mc.root); err != nil {
			return err
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return manifestContents{}, nil, err
	}

	// step 4: update the manifest with table from step 2
	last := mc.lock
	mc.specs = append([]tableSpec{spec}, mc.specs...)
	mc.lock = generateLockHash(mc.root, mc.specs, mc.appendix)

	updated, err := m.Update(ectx, last, mc, &Stats{}, nil)
	if err != nil {
		return manifestContents{}, nil, err
	}

	// step 5
	// the newly created table makes the chunk contents
	// of the existing journal redundant, we can clobber
	// it with the new empty journal from step 3
	wr.path = filepath.Join(nbsPath, chunkJournalAddr)
	if err = os.Rename(tmpJournal, wr.path); err != nil {
		return manifestContents{}, nil, err
	}
	// todo(andy): fsync() nbs dir?

	return updated, wr, nil
}

func validatePersistedManifest(ctx context.Context, m manifest, mc manifestContents) error {
	ok, contents, err := m.ParseIfExists(ctx, &Stats{}, nil)
	if err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("manifest %s missing", m.Name())
	}
	// compare each field except root hash
	if mc.manifestVers != contents.manifestVers ||
		mc.nbfVers != contents.nbfVers ||
		mc.lock != contents.lock ||
		mc.gcGen != contents.gcGen ||
		len(mc.specs) != len(contents.specs) ||
		len(mc.appendix) != len(contents.appendix) {
		return fmt.Errorf("manifest %s does not match memory contents", m.Name())
	}

	ss := toSpecSet(mc.specs)
	for _, spec := range contents.specs {
		if _, ok = ss[spec.name]; !ok {
			return fmt.Errorf("manifest %s does not match memory contents", m.Name())
		}
	}
	as := toSpecSet(mc.appendix)
	for _, spec := range contents.appendix {
		if _, ok = as[spec.name]; !ok {
			return fmt.Errorf("manifest %s does not match memory contents", m.Name())
		}
	}
	return nil
}

func convertJournalToTable(ctx context.Context, nbsPath string) (spec tableSpec, err error) {
	j, err := os.Open(filepath.Join(nbsPath, chunkJournalName))
	if err != nil {
		return tableSpec{}, err
	}

	tmp, err := tempfiles.MovableTempFileProvider.NewFile(nbsPath, tempTablePrefix)
	if err != nil {
		return tableSpec{}, err
	}
	wr := bufio.NewWriterSize(tmp, 65536)

	spec, err = writeJournalToTable(ctx, j, wr)
	if err != nil {
		return tableSpec{}, err
	}

	if err = wr.Flush(); err != nil {
		return tableSpec{}, err
	} else if err = tmp.Sync(); err != nil {
		return tableSpec{}, err
	} else if err = tmp.Close(); err != nil {
		return tableSpec{}, err
	}

	tp := filepath.Join(nbsPath, spec.name.String())
	if err = os.Rename(tmp.Name(), tp); err != nil {
		return tableSpec{}, nil
	}
	return
}
