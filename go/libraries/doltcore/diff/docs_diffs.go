// Copyright 2020 Dolthub, Inc.
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

package diff

import (
	"context"
	"sort"
	"strconv"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

type TableDiffType int

const (
	AddedTable TableDiffType = iota
	ModifiedTable
	RenamedTable
	RemovedTable
)

type DocDiffType int

const (
	AddedDoc DocDiffType = iota
	ModifiedDoc
	RemovedDoc
)

type DocDiffs struct {
	NumAdded    int
	NumModified int
	NumRemoved  int
	DocToType   map[string]DocDiffType
	Docs        []string
}

// NewDocDiffs returns DocDiffs for Dolt Docs between two roots.
func NewDocDiffs(ctx context.Context, older *doltdb.RootValue, newer *doltdb.RootValue, docs doltdocs.Docs) (*DocDiffs, error) {
	var added []string
	var modified []string
	var removed []string
	if older != nil {
		if newer == nil {
			a, m, r, err := DocsDiff(ctx, older, nil, docs)
			if err != nil {
				return nil, err
			}
			added = a
			modified = m
			removed = r
		} else {
			a, m, r, err := DocsDiff(ctx, older, newer, docs)
			if err != nil {
				return nil, err
			}
			added = a
			modified = m
			removed = r
		}
	}
	var docNames []string
	docNames = append(docNames, added...)
	docNames = append(docNames, modified...)
	docNames = append(docNames, removed...)
	sort.Strings(docNames)

	docsToType := make(map[string]DocDiffType)
	for _, nt := range added {
		docsToType[nt] = AddedDoc
	}

	for _, nt := range modified {
		docsToType[nt] = ModifiedDoc
	}

	for _, nt := range removed {
		docsToType[nt] = RemovedDoc
	}

	return &DocDiffs{len(added), len(modified), len(removed), docsToType, docNames}, nil
}

// Len returns the number of docs in a DocDiffs
func (nd *DocDiffs) Len() int {
	return len(nd.Docs)
}

// GetDocDiffs retrieves staged and unstaged DocDiffs.
func GetDocDiffs(
	ctx context.Context,
	roots doltdb.Roots,
	drw env.DocsReadWriter,
) (*DocDiffs, *DocDiffs, error) {
	docsOnDisk, err := drw.GetDocsOnDisk()
	if err != nil {
		return nil, nil, err
	}

	notStagedDocDiffs, err := NewDocDiffs(ctx, roots.Working, nil, docsOnDisk)
	if err != nil {
		return nil, nil, err
	}

	stagedDocDiffs, err := NewDocDiffs(ctx, roots.Head, roots.Staged, docsOnDisk)
	if err != nil {
		return nil, nil, err
	}

	return stagedDocDiffs, notStagedDocDiffs, nil
}

type docComparison struct {
	DocName     string
	CurrentText []byte
	OldText     []byte
}

// DocsDiff returns the added, modified and removed docs when comparing a root value with an other (newer) value. If the other value,
// is not provided, then we compare the docs on the root value to the docs provided.
func DocsDiff(ctx context.Context, root *doltdb.RootValue, other *doltdb.RootValue, docs doltdocs.Docs) (added, modified, removed []string, err error) {
	docComparisons, err := DocsDiffToComparisons(ctx, root, other, docs)

	if err != nil {
		return nil, nil, nil, err
	}

	a, m, r := computeDiffsFromDocComparisons(docComparisons)
	return a, m, r, nil
}

// DocsDiffToComparisons returns the docComparisons between an old root, a new root, and a set of docs. It is exported
// due to the cli usage of doc diffs.
func DocsDiffToComparisons(ctx context.Context, root *doltdb.RootValue, other *doltdb.RootValue, docs doltdocs.Docs) ([]docComparison, error) {
	if other == nil {
		return compareRootWithDocs(ctx, root, docs)
	} else {
		return compareDocsBtwnRoots(ctx, root, other)
	}
}

// compareRootWithDocs compares a root and set of new docs.
func compareRootWithDocs(ctx context.Context, root *doltdb.RootValue, docs doltdocs.Docs) ([]docComparison, error) {
	oldDocs, found, err := doltdocs.GetAllDocs(ctx, root)
	if err != nil {
		return nil, err
	}
	if !found {
		oldDocs = make(doltdocs.Docs, 0)
	}

	return getDocComparisons(oldDocs, docs), nil
}

// compareDocsBtwnRoots takes an oldRoot and a newRoot and compares the docs tables between the two.
func compareDocsBtwnRoots(ctx context.Context, oldRoot *doltdb.RootValue, newRoot *doltdb.RootValue) ([]docComparison, error) {
	oldDocs, found, err := doltdocs.GetAllDocs(ctx, oldRoot)
	if err != nil {
		return nil, err
	}
	if !found {
		oldDocs = make(doltdocs.Docs, 0)
	}

	newDocs, found, err := doltdocs.GetAllDocs(ctx, newRoot)
	if err != nil {
		return nil, err
	}
	if !found {
		newDocs = make(doltdocs.Docs, 0)
	}

	return getDocComparisons(oldDocs, newDocs), nil
}

// getDocComparisons compares two sets of docs looking for modifications, removals, and additions as docComparisons
func getDocComparisons(oldDocs doltdocs.Docs, newDocs doltdocs.Docs) []docComparison {
	docComparisons := make([]docComparison, 0)

	// First case is looking at the old docs and seeing what was modified or removed
	for _, oldDoc := range oldDocs {
		dc := docComparison{DocName: oldDoc.DocPk, OldText: oldDoc.Text, CurrentText: getMatchingText(oldDoc, newDocs)}
		docComparisons = append(docComparisons, dc)
	}

	// Second case is looking back at the old docs and seeing what was added
	for _, newDoc := range newDocs {
		oldText := getMatchingText(newDoc, oldDocs)
		if oldText == nil {
			dc := docComparison{DocName: newDoc.DocPk, OldText: nil, CurrentText: newDoc.Text}
			docComparisons = append(docComparisons, dc)
		}
	}

	return docComparisons
}

// getMatchingText matches a doc in a set of other docs and returns the relevant text.
func getMatchingText(doc doltdocs.Doc, docs doltdocs.Docs) []byte {
	for _, toCompare := range docs {
		if doc.DocPk == toCompare.DocPk {
			return toCompare.Text
		}
	}

	return nil
}

// computeDiffsFromDocComparisons takes the docComparisons and returns the final add, modified, removed count.
func computeDiffsFromDocComparisons(docComparisons []docComparison) (added, modified, removed []string) {
	added = []string{}
	modified = []string{}
	removed = []string{}
	for _, doc := range docComparisons {
		added, modified, removed = appendDocDiffs(added, modified, removed, doc.OldText, doc.CurrentText, doc.DocName)
	}
	return added, modified, removed
}

func appendDocDiffs(added, modified, removed []string, olderVal []byte, newerVal []byte, docPk string) (add, mod, rem []string) {
	if olderVal == nil && newerVal != nil {
		added = append(added, docPk)
	} else if olderVal != nil {
		if newerVal == nil {
			removed = append(removed, docPk)
		} else if strconv.Quote(string(olderVal)) != strconv.Quote(string(newerVal)) {
			modified = append(modified, docPk)
		}
	}
	return added, modified, removed
}
