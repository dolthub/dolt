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

package env

import (
	"context"
	"errors"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

type Docs []doltdb.DocDetails

var doltDocsColumns, _ = schema.NewColCollection(
	schema.NewColumn(doltdb.DocPkColumnName, schema.DocNameTag, types.StringKind, true, schema.NotNullConstraint{}),
	schema.NewColumn(doltdb.DocTextColumnName, schema.DocTextTag, types.StringKind, false),
)
var DoltDocsSchema = schema.MustSchemaFromCols(doltDocsColumns)

// AllValidDocDetails is a list of all valid docs with static fields DocPk and File. All other DocDetail fields
// are dynamic and must be added, modified or removed as needed.
var AllValidDocDetails = &Docs{
	doltdb.DocDetails{DocPk: doltdb.ReadmePk, File: ReadmeFile},
	doltdb.DocDetails{DocPk: doltdb.LicensePk, File: LicenseFile},
}

func LoadDocs(fs filesys.ReadWriteFS) (Docs, error) {
	docsWithCurrentText := *AllValidDocDetails
	for i, val := range docsWithCurrentText {
		path := getDocFile(val.File)
		exists, isDir := fs.Exists(path)
		if exists && !isDir {
			data, err := fs.ReadFile(path)
			if err != nil {
				return nil, err
			}
			val.NewerText = data
			docsWithCurrentText[i] = val
		}
	}
	return docsWithCurrentText, nil
}

func (docs Docs) Save(fs filesys.ReadWriteFS) error {
	for _, doc := range docs {
		if !IsValidDoc(doc.DocPk) {
			continue
		}
		filePath := getDocFile(doc.File)
		if doc.NewerText != nil {
			err := fs.WriteFile(filePath, doc.NewerText)
			if err != nil {
				return err
			}
		} else {
			err := DeleteDoc(fs, doc.DocPk)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func DeleteDoc(fs filesys.ReadWriteFS, docName string) error {
	for _, doc := range *AllValidDocDetails {
		if doc.DocPk == docName {
			path := getDocFile(doc.File)
			exists, isDir := fs.Exists(path)
			if exists && !isDir {
				return fs.DeleteFile(path)
			}
		}
	}
	return nil
}

func IsValidDoc(docName string) bool {
	for _, doc := range *AllValidDocDetails {
		if doc.DocPk == docName {
			return true
		}
	}
	return false
}

func hasDocFile(fs filesys.ReadWriteFS, file string) bool {
	exists, isDir := fs.Exists(getDocFile(file))
	return exists && !isDir
}

// WorkingRootWithDocs returns a copy of the working root that has been updated with the Dolt docs from the file system.
func WorkingRootWithDocs(ctx context.Context, dbData DbData) (*doltdb.RootValue, error) {
	drw := dbData.Drw

	dds, err := drw.GetDocsOnDisk()
	if err != nil {
		return nil, err
	}

	working, err := WorkingRoot(ctx, dbData.Ddb, dbData.Rsr)
	if err != nil {
		return nil, err
	}

	return UpdateRootWithDocs(ctx, dbData, working, Working, dds)
}

// UpdateRootWithDocs takes in a root value, a drw, and some docs and writes those docs to the dolt_docs table
// (perhaps creating it in the process). The table might not necessarily need to be created if there are no docs in the
// repo yet.
func UpdateRootWithDocs(ctx context.Context, dbData DbData, root *doltdb.RootValue, rootType RootType, docDetails []doltdb.DocDetails) (*doltdb.RootValue, error) {
	docTbl, _, err := root.GetTable(ctx, doltdb.DocTableName)

	if err != nil {
		return nil, err
	}

	docTbl, err = dbData.Drw.WriteDocsToDisk(ctx, root.VRW(), docTbl, docDetails)

	if errors.Is(ErrEmptyDocsTable, err) {
		root, err = root.RemoveTables(ctx, doltdb.DocTableName)
	} else if err != nil {
		return nil, err
	}

	// There might not need be a need to create docs table if not docs have been created yet so check if docTbl != nil.
	if docTbl != nil {
		root, err = root.PutTable(ctx, doltdb.DocTableName, docTbl)
	}

	switch rootType {
	case Working:
		_, err = UpdateWorkingRoot(ctx, dbData.Ddb, dbData.Rsw, root)
	case Staged:
		_, err = UpdateStagedRoot(ctx, dbData.Ddb, dbData.Rsw, root)
	default:
		return nil, errors.New("Root type not supported with docs update.")
	}
	return root, nil
}

// ResetWorkingDocsToStagedDocs resets the `dolt_docs` table on the working root to match the staged root.
// If the `dolt_docs` table does not exist on the staged root, it will be removed from the working root.
func ResetWorkingDocsToStagedDocs(ctx context.Context, ddb *doltdb.DoltDB, rsr RepoStateReader, rsw RepoStateWriter) error {
	wrkRoot, err := WorkingRoot(ctx, ddb, rsr)
	if err != nil {
		return err
	}

	stgRoot, err := StagedRoot(ctx, ddb, rsr)
	if err != nil {
		return err
	}

	stgDocTbl, stgDocsFound, err := stgRoot.GetTable(ctx, doltdb.DocTableName)
	if err != nil {
		return err
	}

	_, wrkDocsFound, err := wrkRoot.GetTable(ctx, doltdb.DocTableName)
	if err != nil {
		return err
	}

	if wrkDocsFound && !stgDocsFound {
		newWrkRoot, err := wrkRoot.RemoveTables(ctx, doltdb.DocTableName)
		if err != nil {
			return err
		}
		_, err = UpdateWorkingRoot(ctx, ddb, rsw, newWrkRoot)
		return err
	}

	if stgDocsFound {
		newWrkRoot, err := wrkRoot.PutTable(ctx, doltdb.DocTableName, stgDocTbl)
		if err != nil {
			return err
		}
		_, err = UpdateWorkingRoot(ctx, ddb, rsw, newWrkRoot)
		return err
	}
	return nil
}

// GetDocsWithNewerTextFromRoot returns Docs with the NewerText value(s) from the provided root. If docs are provided,
// only those docs will be retrieved and returned. Otherwise, all valid doc details are returned with the updated NewerText.
func GetDocsWithNewerTextFromRoot(ctx context.Context, root *doltdb.RootValue, docs Docs) (Docs, error) {
	docTbl, docTblFound, err := root.GetTable(ctx, doltdb.DocTableName)
	if err != nil {
		return nil, err
	}

	var sch schema.Schema
	if docTblFound {
		docSch, err := docTbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		sch = docSch
	}

	if docs == nil {
		docs = *AllValidDocDetails
	}

	for i, doc := range docs {
		doc, err = doltdb.AddNewerTextToDocFromTbl(ctx, docTbl, &sch, doc)
		if err != nil {
			return nil, err
		}
		docs[i] = doc
	}
	return docs, nil
}
