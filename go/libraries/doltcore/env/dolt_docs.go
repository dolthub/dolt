// Copyright 2019 Liquidata, Inc.
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
	"os"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

var initialReadme = "This is a repository level README. Either edit it, add it, and commit it, or remove the file."
var initialLicense = "This is a repository level LICENSE. Either edit it, add it, and commit it, or remove the file."

type Docs []*doltdb.DocDetails

// AllValidDocDetails is a list of all valid docs with static fields DocPk and File. All other DocDetail fields
// are dynamic and must be added, modified or removed as needed.
var AllValidDocDetails = &Docs{
	&doltdb.DocDetails{DocPk: doltdb.ReadmePk, File: ReadmeFile},
	&doltdb.DocDetails{DocPk: doltdb.LicensePk, File: LicenseFile},
}

func LoadDocs(fs filesys.ReadWriteFS) (*Docs, error) {
	docsWithCurrentText := *AllValidDocDetails
	for _, val := range docsWithCurrentText {
		path := getDocFile(val.File)
		exists, isDir := fs.Exists(path)
		if exists && !isDir {
			data, err := fs.ReadFile(path)
			if err != nil {
				return nil, err
			}
			val.NewerText = data
		}
	}
	return &docsWithCurrentText, nil
}

func CreateDocs(fs filesys.ReadWriteFS) (*Docs, error) {
	docs := *AllValidDocDetails
	for _, doc := range docs {
		doc.NewerText = getInitialDocText(doc.DocPk)
	}
	err := docs.Save(fs)
	if err != nil {
		return nil, err
	}
	return &docs, nil
}

func (docs *Docs) Save(fs filesys.ReadWriteFS) error {
	for _, doc := range *docs {
		if !IsValidDoc(doc.DocPk) {
			continue
		}
		filePath := getDocFile(doc.File)
		if doc.NewerText != nil {
			err := fs.WriteFile(filePath, doc.NewerText)
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
				return os.Remove(path)
			}
		}
	}
	return nil
}

func getInitialDocText(docName string) []byte {
	switch docName {
	case doltdb.ReadmePk:
		return []byte(initialReadme)
	case doltdb.LicensePk:
		return []byte(initialLicense)
	default:
		return nil
	}
}

func IsValidDoc(docName string) bool {
	for _, doc := range *AllValidDocDetails {
		if doc.DocPk == docName {
			return true
		}
	}
	return false
}
