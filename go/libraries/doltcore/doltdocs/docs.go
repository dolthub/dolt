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
	"path/filepath"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	ReadmeFile  = "../README.md"
	LicenseFile = "../LICENSE.md"
)

type DocDetails struct {
	Text  []byte
	DocPk string
	File  string
}

type Docs []DocDetails

var AllValidDocDetails = &Docs{
	{DocPk: doltdb.ReadmePk, File: ReadmeFile},
	{DocPk: doltdb.LicensePk, File: LicenseFile},
}

func GetDocFile(filename string) string {
	return filepath.Join(dbfactory.DoltDir, filename)
}

func LoadDocs(fs filesys.ReadWriteFS) (Docs, error) {
	docsWithCurrentText := *AllValidDocDetails
	for i, val := range docsWithCurrentText {
		path := GetDocFile(val.File)
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

func (docs Docs) Save(fs filesys.ReadWriteFS) error {
	for _, doc := range docs {
		if !IsValidDoc(doc.DocPk) {
			continue
		}
		filePath := GetDocFile(doc.File)
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

func DeleteDoc(fs filesys.ReadWriteFS, docName string) error {
	for _, doc := range *AllValidDocDetails {
		if doc.DocPk == docName {
			path := GetDocFile(doc.File)
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

func DocTblKeyFromName(fmt *types.NomsBinFormat, name string) (types.Tuple, error) {
	return types.NewTuple(fmt, types.Uint(schema.DocNameTag), types.String(name))
}

// GetDocRow returns the associated row of a particular doc from noms.
func GetDocRow(ctx context.Context, docTbl *doltdb.Table, sch schema.Schema, key types.Tuple) (r row.Row, ok bool, err error) {
	rowMap, err := docTbl.GetRowData(ctx)
	if err != nil {
		return nil, false, err
	}

	var fields types.Value
	fields, ok, err = rowMap.MaybeGet(ctx, key)
	if err != nil || !ok {
		return nil, ok, err
	}

	r, err = row.FromNoms(sch, key, fields.(types.Tuple))
	return
}

// GetDocTextFromTbl updates the Text field of a docDetail using the provided table and schema.
func GetDocTextFromTbl(ctx context.Context, tbl *doltdb.Table, sch *schema.Schema, doc DocDetails) (DocDetails, error) {
	if tbl != nil && sch != nil {
		key, err := DocTblKeyFromName(tbl.Format(), doc.DocPk)
		if err != nil {
			return DocDetails{}, err
		}

		docRow, ok, err := GetDocRow(ctx, tbl, *sch, key)
		if err != nil {
			return DocDetails{}, err
		}
		if ok {
			docValue, _ := docRow.GetColVal(schema.DocTextTag)
			doc.Text = []byte(docValue.(types.String))
		} else {
			doc.Text = nil
		}
	} else {
		doc.Text = nil
	}
	return doc, nil
}
