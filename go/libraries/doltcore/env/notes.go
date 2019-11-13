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
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

var initialReadme = "This is a repository level README. Either edit it, add it, and commit it, or remove the file."
var initialLicense = "This is a repository level LICENSE. Either edit it, add it, and commit it, or remove the file."

type Notes struct {
	key map[string]string

	fs filesys.ReadWriteFS
}

func LoadNotes(fs filesys.ReadWriteFS) (*Notes, error) {
	readmePath := getReadmeFile()
	var readmeData []byte
	if readmePath != "" {
		data, err := fs.ReadFile(readmePath)
		if err != nil {
			return nil, err
		}
		readmeData = data
	}

	licensePath := getLicenseFile()
	var licenseData []byte
	if licensePath != "" {
		data, err := fs.ReadFile(licensePath)
		if err != nil {
			return nil, err
		}
		licenseData = data
	}

	var notes Notes
	notesMap := map[string]string{
		"readme":  string(readmeData),
		"license": string(licenseData),
	}
	notes.key = notesMap
	notes.fs = fs

	return &notes, nil
}

func CreateNotes(fs filesys.ReadWriteFS) (*Notes, error) {
	notesMap := map[string]string{
		"readme":  initialReadme,
		"license": initialLicense,
	}
	nts := &Notes{notesMap, fs}
	err := nts.Save()
	if err != nil {
		return nil, err
	}
	return nts, nil
}

func (nts *Notes) Save() error {
	readmePath := getReadmeFile()
	licensePath := getLicenseFile()

	err := nts.fs.WriteFile(readmePath, []byte(nts.key["readme"]))
	if err != nil {
		return err
	}
	err = nts.fs.WriteFile(licensePath, []byte(nts.key["license"]))
	if err != nil {
		return err
	}
	return nil
}
