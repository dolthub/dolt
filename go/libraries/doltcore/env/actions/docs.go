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

package actions

import (
	"context"
	"errors"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

// SaveTrackedDocsFromWorking saves docs from the working root to the filesystem, and doesn't modify untracked docs.
func SaveTrackedDocsFromWorking(ctx context.Context, dEnv *env.DoltEnv) error {
	localDocs := dEnv.Docs
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}

	return SaveTrackedDocs(ctx, dEnv.DocsReadWriter(), workingRoot, workingRoot, localDocs)
}

// SaveDocsFromRoot saves docs from the root given to the filesystem, and could overwrite untracked docs.
func SaveDocsFromRoot(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv) error {
	localDocs := dEnv.Docs
	drw := dEnv.DocsReadWriter()

	docs, err := doltdocs.GetDocsFromRoot(ctx, root, doltdocs.GetDocNamesFromDocs(doltdocs.SupportedDocs)...)
	if err != nil {
		return err
	}

	err = drw.WriteDocsToDisk(docs)
	if err != nil {
		// If we can't update docs on disk, attempt to revert the change
		drw.WriteDocsToDisk(localDocs)
		return err
	}

	return nil
}

// SaveTrackedDocs writes the docs from the targetRoot to the filesystem. The working root is used to identify untracked docs, which are left unchanged.
func SaveTrackedDocs(ctx context.Context, drw env.DocsReadWriter, workRoot, targetRoot *doltdb.RootValue, localDocs doltdocs.Docs) error {
	docDiffs, err := diff.NewDocDiffs(ctx, workRoot, nil, localDocs)
	if err != nil {
		return err
	}

	docs := removeUntrackedDocs(localDocs, docDiffs)

	docs, err = doltdocs.GetDocsFromRoot(ctx, targetRoot, doltdocs.GetDocNamesFromDocs(docs)...)
	if err != nil {
		return err
	}

	err = drw.WriteDocsToDisk(docs)

	if err != nil {
		// If we can't update docs on disk, attempt to revert the change
		_ = drw.WriteDocsToDisk(localDocs)
		return err
	}

	return nil
}

func docIsUntracked(doc string, untracked []string) bool {
	for _, val := range untracked {
		if doc == val {
			return true
		}
	}
	return false
}

func removeUntrackedDocs(docs doltdocs.Docs, docDiffs *diff.DocDiffs) doltdocs.Docs {
	result := doltdocs.Docs{}
	untracked := getUntrackedDocs(docs, docDiffs)

	for _, doc := range docs {
		if !docIsUntracked(doc.DocPk, untracked) {
			result = append(result, doc)
		}
	}
	return result
}

func getUntrackedDocs(docs doltdocs.Docs, docDiffs *diff.DocDiffs) []string {
	untracked := []string{}
	for _, docName := range docDiffs.Docs {
		dt := docDiffs.DocToType[docName]
		if dt == diff.AddedDoc {
			untracked = append(untracked, docName)
		}
	}

	return untracked
}

func getUpdatedWorkingAndStagedWithDocs(ctx context.Context, roots doltdb.Roots, docs doltdocs.Docs) (doltdb.Roots, doltdocs.Docs, error) {
	docsRoot := roots.Head
	_, ok, err := roots.Staged.GetTable(ctx, doltdb.DocTableName)
	if err != nil {
		return doltdb.Roots{}, nil, err
	} else if ok {
		docsRoot = roots.Staged
	}

	docs, err = doltdocs.GetDocsFromRoot(ctx, docsRoot, doltdocs.GetDocNamesFromDocs(docs)...)
	if err != nil {
		return doltdb.Roots{}, nil, err
	}

	roots.Working, err = doltdocs.UpdateRootWithDocs(ctx, roots.Working, docs)
	if err != nil {
		return doltdb.Roots{}, nil, err
	}

	roots.Staged, err = doltdocs.UpdateRootWithDocs(ctx, roots.Staged, docs)
	if err != nil {
		return doltdb.Roots{}, nil, err
	}

	return roots, docs, nil
}

// GetUnstagedDocs retrieves the unstaged docs (docs from the filesystem).
func GetUnstagedDocs(ctx context.Context, dEnv *env.DoltEnv) (doltdocs.Docs, error) {
	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return nil, err
	}

	_, unstagedDocDiffs, err := diff.GetDocDiffs(ctx, roots, dEnv.DocsReadWriter())
	if err != nil {
		return nil, err
	}

	unstagedDocs := doltdocs.Docs{}
	for _, docName := range unstagedDocDiffs.Docs {
		docAr, err := dEnv.DocsReadWriter().GetDocsOnDisk(docName)
		if err != nil {
			return nil, err
		}
		if len(docAr) < 1 {
			return nil, errors.New("error: Failed getting unstaged docs")
		}

		unstagedDocs = append(unstagedDocs, docAr[0])
	}
	return unstagedDocs, nil
}

// SaveDocsFromWorkingExcludingFSChanges saves docs from the working root to the filesystem, and does not overwrite changes to docs on the FS.
// Intended to be called after checking that no conflicts exist (during a checkout or merge, i.e.).
func SaveDocsFromWorkingExcludingFSChanges(ctx context.Context, dEnv *env.DoltEnv, docsToExclude doltdocs.Docs) error {
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}

	var docsToSave doltdocs.Docs
	if len(docsToExclude) > 0 {
		for _, doc := range dEnv.Docs {
			for _, excludedDoc := range docsToExclude {
				if doc.DocPk != excludedDoc.DocPk {
					docsToSave = append(docsToSave, doc)
				}
			}
		}
	} else {
		docsToSave = dEnv.Docs
	}

	return SaveTrackedDocs(ctx, dEnv.DocsReadWriter(), workingRoot, workingRoot, docsToSave)
}

// GetTablesOrDocs takes a slice of table or file names. Table names are returned as given. Supported doc names are
// read from disk and their name replace with the names of the dolt_docs system table in the input slice. Supported docs
// are returned in the second return param.
func GetTablesOrDocs(drw env.DocsReadWriter, tablesOrFiles []string) (tables []string, docs doltdocs.Docs, err error) {
	for i, tbl := range tablesOrFiles {
		if _, ok := doltdocs.IsSupportedDoc(tbl); ok {
			docAr, err := drw.GetDocsOnDisk(tbl)
			if err != nil {
				return nil, nil, err
			}
			if len(docAr) < 1 {
				return nil, nil, errors.New("error: Failed getting docs")
			}

			doc := docAr[0]
			if doc.DocPk == "" {
				return nil, nil, errors.New("Supported doc not found on disk.")
			}
			docs = append(docs, doc)
			tablesOrFiles[i] = doltdb.DocTableName
		}
	}
	return tablesOrFiles, docs, nil
}
