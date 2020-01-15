// Copyright 2020 Liquidata, Inc.
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

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
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

// SaveTrackedDocs writes the docs from the targetRoot to the filesystem. The working root is used to identify untracked docs, which are left unchanged.
func SaveTrackedDocs(ctx context.Context, dEnv *env.DoltEnv, workRoot, targetRoot *doltdb.RootValue, localDocs env.Docs) error {
	docDiffs, err := NewDocDiffs(ctx, dEnv, workRoot, nil, localDocs)
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

// SaveDocsFromWorking saves docs from the working root to the filesystem.
// Untracked docs will be overwritten if there is not corresponding doc value on the root.
func SaveDocsFromWorking(ctx context.Context, dEnv *env.DoltEnv) error {
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}

	err = dEnv.UpdateFSDocsToRootDocs(ctx, workingRoot, nil)
	if err != nil {
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

func removeUntrackedDocs(docs []doltdb.DocDetails, docDiffs *DocDiffs) []doltdb.DocDetails {
	result := []doltdb.DocDetails{}
	untracked := getUntrackedDocs(docs, docDiffs)

	for _, doc := range docs {
		if !docIsUntracked(doc.DocPk, untracked) {
			result = append(result, doc)
		}
	}
	return result
}

func getUntrackedDocs(docs []doltdb.DocDetails, docDiffs *DocDiffs) []string {
	untracked := []string{}
	for _, docName := range docDiffs.Docs {
		dt := docDiffs.DocToType[docName]
		if dt == AddedDoc {
			untracked = append(untracked, docName)
		}
	}

	return untracked
}
