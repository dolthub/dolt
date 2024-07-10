// Copyright 2024 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

type StorageType int

const (
	Journal StorageType = iota
	TableFileNewGen
	TableFileOldGen
	Archive
)

type ArchiveMetadata struct {
	originalTableFileId string
}

type StorageArtifact struct {
	path        string
	storageType StorageType
	arcMetadata *ArchiveMetadata
}

type StorageMetadata struct {
	artifacts []StorageArtifact
}

func (sm *StorageMetadata) ArchiveFilesPresent() bool {
	for _, artifact := range sm.artifacts {
		if artifact.storageType == Archive {
			return true
		}
	}
	return false
}

// GetStorageMetadata returns metadata about the local filesystem storage for a single database. The path given must be
// the path to DB directory - ie, containing the .dolt directory.
func GetStorageMetadata(path string) (StorageMetadata, error) {
	err := validateDir(path)
	if err != nil {
		return StorageMetadata{}, err
	}

	//	newGen := filepath.Join(path, ".dolt", "noms")
	//	newgenManifest := filepath.Join(newGen, "manifest")

	oldgen := filepath.Join(path, ".dolt", "noms", "oldgen")
	oldgenManifest := filepath.Join(oldgen, "manifest")

	// create a io.Reader for the manifest file
	manifestReader, err := os.Open(oldgenManifest)
	if err != nil {
		return StorageMetadata{}, err
	}

	manifest, err := ParseManifest(manifestReader)
	if err != nil {
		return StorageMetadata{}, err
	}

	var artifacts []StorageArtifact

	// for each table in the manifest, get the table spec
	for i := 0; i < manifest.NumTableSpecs(); i++ {
		tableSpecInfo := manifest.GetTableSpecInfo(i)

		// If the oldgen/name exists, it's not an archive. If it doesn't exist with a .darc suffix, then it's an archive.
		tfName := tableSpecInfo.GetName()
		fullPath := filepath.Join(oldgen, tfName)
		_, err := os.Stat(fullPath)
		if err == nil {
			// exists.  Not an archive.
			artifacts = append(artifacts, StorageArtifact{
				path:        fullPath,
				storageType: TableFileOldGen,
			})
		} else if os.IsNotExist(err) {
			arcName := tfName + ".darc"
			arcPath := filepath.Join(oldgen, arcName)
			_, err := os.Stat(arcPath)
			if err == nil {
				// reader for the path. State. call
				reader, fileSize, err := openReader(arcPath)
				if err != nil {
					return StorageMetadata{}, err
				}

				arcMetadata, err := newArchiveMetadata(reader, fileSize)
				if err != nil {
					return StorageMetadata{}, err
				}

				artifacts = append(artifacts, StorageArtifact{
					path:        arcPath,
					storageType: Archive,
					arcMetadata: arcMetadata,
				})
			} else {
				// any error is bad here. If the files don't exist, then the manifest is no good.
				return StorageMetadata{}, err
			}
		} else {
			// some other error.
			return StorageMetadata{}, err
		}
	}

	return StorageMetadata{artifacts}, nil
}

func validateDir(path string) error {
	info, err := os.Stat(path)

	if err != nil {
		return err
	} else if !info.IsDir() {
		return filesys.ErrIsFile
	}

	return nil
}
