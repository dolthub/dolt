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

package doltdocs

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrDocsUpdate = errors.New("error updating local docs")
var ErrEmptyDocsTable = errors.New("error: All docs removed. Removing Docs Table")
var ErrMarshallingSchema = errors.New("error marshalling schema")

var doltDocsColumns = schema.NewColCollection(
	schema.NewColumn(doltdb.DocPkColumnName, schema.DocNameTag, types.StringKind, true, schema.NotNullConstraint{}),
	schema.NewColumn(doltdb.DocTextColumnName, schema.DocTextTag, types.StringKind, false),
)
var DocsSchema = schema.MustSchemaFromCols(doltDocsColumns)

type Doc struct {
	Text  []byte
	DocPk string
	File  string
}

type Docs []Doc

const (
	ReadmeFile  = "../README.md"
	LicenseFile = "../LICENSE.md"

	// LicenseDoc is the key for accessing the license within the docs table
	LicenseDoc = "LICENSE.md"
	// ReadmeDoc is the key for accessing the readme within the docs table
	ReadmeDoc = "README.md"
)

var SupportedDocs = Docs{
	{DocPk: ReadmeDoc, File: ReadmeFile},
	{DocPk: LicenseDoc, File: LicenseFile},
}

// GetLocalFileText returns a byte slice representing the contents of the provided file, if it exists
func GetLocalFileText(fs filesys.Filesys, file string) ([]byte, error) {
	path := ""
	if DocFileExists(fs, file) {
		path = GetDocFilePath(file)
	}

	if path != "" {
		return fs.ReadFile(path)
	}

	return nil, nil
}

// GetSupportedDocs takes in a filesystem and returns the contents of all docs on disk.
func GetSupportedDocs(fs filesys.Filesys) (docs Docs, err error) {
	docs = Docs{}
	for _, doc := range SupportedDocs {
		newerText, err := GetLocalFileText(fs, doc.File)
		if err != nil {
			return nil, err
		}
		doc.Text = newerText
		docs = append(docs, doc)
	}
	return docs, nil
}

// GetDoc takes in a filesystem and a docName and returns the doc's contents.
func GetDoc(fs filesys.Filesys, docName string) (doc Doc, err error) {
	for _, doc := range SupportedDocs {
		if doc.DocPk == docName {
			newerText, err := GetLocalFileText(fs, doc.File)
			if err != nil {
				return Doc{}, err
			}
			doc.Text = newerText
			return doc, nil
		}
	}
	return Doc{}, err
}

func GetDocNamesFromDocs(docs Docs) []string {
	if docs == nil {
		return nil
	}

	ret := make([]string, len(docs))

	for i, doc := range docs {
		ret[i] = doc.DocPk
	}

	return ret
}

// GetDocsFromRoot takes in a root value and returns the docs stored in its dolt_docs table.
func GetDocsFromRoot(ctx context.Context, root *doltdb.RootValue, docNames ...string) (Docs, error) {
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

	if docNames == nil {
		docNames = GetDocNamesFromDocs(SupportedDocs)
	}

	docs := make(Docs, len(docNames))
	for i, name := range docNames {
		doc, isSupported := IsSupportedDoc(name)
		if !isSupported {
			return nil, fmt.Errorf("%s is not a supported doc", name)
		}

		docText, err := getDocTextFromTbl(ctx, docTbl, &sch, name)
		if err != nil {
			return nil, err
		}

		doc.Text = docText
		docs[i] = doc
	}

	return docs, nil
}

// Save takes in a fs object and saves all the docs to the filesystem, overwriting any existing files.
func (docs Docs) Save(fs filesys.ReadWriteFS) error {
	for _, doc := range docs {
		if _, ok := IsSupportedDoc(doc.DocPk); !ok {
			continue
		}
		filePath := GetDocFilePath(doc.File)
		if doc.Text != nil {
			err := fs.WriteFile(filePath, doc.Text)
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

// GetDocFilePath takes in a filename and appends it to the DoltDir filepath.
func GetDocFilePath(filename string) string {
	return filepath.Join(dbfactory.DoltDir, filename)
}

// LoadDocs takes in a fs object and reads all the docs (ex. README.md) defined in SupportedDocs.
func LoadDocs(fs filesys.ReadWriteFS) (Docs, error) {
	docsWithCurrentText := SupportedDocs
	for i, val := range docsWithCurrentText {
		path := GetDocFilePath(val.File)
		exists, isDir := fs.Exists(path)
		if exists && !isDir {
			data, err := fs.ReadFile(path)
			if err != nil {
				return nil, err
			}
			val.Text = data
			docsWithCurrentText[i] = val
		}
	}
	return docsWithCurrentText, nil
}

func IsSupportedDoc(docName string) (Doc, bool) {
	for _, doc := range SupportedDocs {
		if doc.DocPk == docName {
			return doc, true
		}
	}
	return Doc{}, false
}

func DocFileExists(fs filesys.ReadWriteFS, file string) bool {
	exists, isDir := fs.Exists(GetDocFilePath(file))
	return exists && !isDir
}

// DeleteDoc takes in a filesytem object and deletes the file with docName, if it's a SupportedDoc.
func DeleteDoc(fs filesys.ReadWriteFS, docName string) error {
	if doc, ok := IsSupportedDoc(docName); ok {
		if doc.DocPk == docName {
			path := GetDocFilePath(doc.File)
			exists, isDir := fs.Exists(path)
			if exists && !isDir {
				return fs.DeleteFile(path)
			}
		}
	}
	return nil
}

// UpdateRootWithDocs takes in a root value, and some docs and writes those docs to the dolt_docs table
// (perhaps creating it in the process). The table might not necessarily need to be created if there are no docs in the
// repo yet.
func UpdateRootWithDocs(ctx context.Context, root *doltdb.RootValue, docs Docs) (*doltdb.RootValue, error) {
	docTbl, err := CreateOrUpdateDocsTable(ctx, root, docs)

	if errors.Is(ErrEmptyDocsTable, err) {
		root, err = root.RemoveTables(ctx, false, false, doltdb.DocTableName)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	// There might not need be a need to create docs table if not docs have been created yet so check if docTbl != nil.
	if docTbl != nil {
		root, err = root.PutTable(ctx, doltdb.DocTableName, docTbl)
		if err != nil {
			return nil, err
		}
	}

	return root, nil
}
