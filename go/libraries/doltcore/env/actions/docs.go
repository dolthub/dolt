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

	return SaveTrackedDocs(ctx, dEnv, workingRoot, workingRoot, localDocs)
}

// SaveDocsFromWorking saves docs from the working root to the filesystem, and could overwrite untracked docs.
func SaveDocsFromWorking(ctx context.Context, dEnv *env.DoltEnv) error {
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}

	return SaveDocsFromRoot(ctx, workingRoot, dEnv)
}

// SaveDocsFromRoot saves docs from the root given to the filesystem, and could overwrite untracked docs.
func SaveDocsFromRoot(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv) error {
	localDocs := dEnv.Docs

	err := dEnv.UpdateFSDocsToRootDocs(ctx, root, nil)
	if err != nil {
		// If we can't update docs on disk, attempt to revert the change
		localDocs.Save(dEnv.FS)
		return err
	}

	return nil
}

// SaveTrackedDocs writes the docs from the targetRoot to the filesystem. The working root is used to identify untracked docs, which are left unchanged.
func SaveTrackedDocs(ctx context.Context, dEnv *env.DoltEnv, workRoot, targetRoot *doltdb.RootValue, localDocs env.Docs) error {
	docDiffs, err := diff.NewDocDiffs(ctx, workRoot, nil, localDocs)
	if err != nil {
		return err
	}

	docs := removeUntrackedDocs(localDocs, docDiffs)

	err = dEnv.UpdateFSDocsToRootDocs(ctx, targetRoot, docs)
	if err != nil {
		localDocs.Save(dEnv.FS)
		return err
	}

	return nil
}

// SaveDocsFromDocDetails saves the provided docs to the filesystem.
// An untracked doc will be overwritten if doc.NewerText == nil.
func SaveDocsFromDocDetails(dEnv *env.DoltEnv, docs env.Docs) error {
	return docs.Save(dEnv.FS)
}

func docIsUntracked(doc string, untracked []string) bool {
	for _, val := range untracked {
		if doc == val {
			return true
		}
	}
	return false
}

func removeUntrackedDocs(docs []doltdb.DocDetails, docDiffs *diff.DocDiffs) []doltdb.DocDetails {
	result := []doltdb.DocDetails{}
	untracked := getUntrackedDocs(docs, docDiffs)

	for _, doc := range docs {
		if !docIsUntracked(doc.DocPk, untracked) {
			result = append(result, doc)
		}
	}
	return result
}

func getUntrackedDocs(docs []doltdb.DocDetails, docDiffs *diff.DocDiffs) []string {
	untracked := []string{}
	for _, docName := range docDiffs.Docs {
		dt := docDiffs.DocToType[docName]
		if dt == diff.AddedDoc {
			untracked = append(untracked, docName)
		}
	}

	return untracked
}

func getUpdatedWorkingAndStagedWithDocs(ctx context.Context, dEnv *env.DoltEnv, working, staged, head *doltdb.RootValue, docDetails []doltdb.DocDetails) (currRoot, stgRoot *doltdb.RootValue, err error) {
	root := head
	_, ok, err := staged.GetTable(ctx, doltdb.DocTableName)
	if err != nil {
		return nil, nil, err
	} else if ok {
		root = staged
	}

	docs, err := dEnv.GetDocsWithNewerTextFromRoot(ctx, root, docDetails)
	if err != nil {
		return nil, nil, err
	}

	currRoot, err = dEnv.GetUpdatedRootWithDocs(ctx, working, docs)
	if err != nil {
		return nil, nil, err
	}

	stgRoot, err = dEnv.GetUpdatedRootWithDocs(ctx, staged, docs)
	if err != nil {
		return nil, nil, err
	}

	return currRoot, stgRoot, nil
}

// GetUnstagedDocs retrieves the unstaged docs (docs from the filesystem).
func GetUnstagedDocs(ctx context.Context, dEnv *env.DoltEnv) (env.Docs, error) {
	_, unstagedDocDiffs, err := diff.GetDocDiffs(ctx, dEnv.DoltDB, dEnv.RepoStateReader())
	if err != nil {
		return nil, err
	}
	unstagedDocs := env.Docs{}
	for _, docName := range unstagedDocDiffs.Docs {
		docDetail, err := dEnv.GetOneDocDetail(docName)
		if err != nil {
			return nil, err
		}
		unstagedDocs = append(unstagedDocs, docDetail)
	}
	return unstagedDocs, nil
}

// SaveDocsFromWorkingExcludingFSChanges saves docs from the working root to the filesystem, and does not overwrite changes to docs on the FS.
// Intended to be called after checking that no conflicts exist (during a checkout or merge, i.e.).
func SaveDocsFromWorkingExcludingFSChanges(ctx context.Context, dEnv *env.DoltEnv, docsToExclude env.Docs) error {
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}

	var docsToSave env.Docs
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

	return SaveTrackedDocs(ctx, dEnv, workingRoot, workingRoot, docsToSave)
}
