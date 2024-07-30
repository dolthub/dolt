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

package nbs

import (
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
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
	id          hash.Hash
	path        string
	storageType StorageType
	arcMetadata *ArchiveMetadata
}

type StorageMetadata struct {
	root      string
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

// RevertMap returns a map of Archive file ids to their origin TableFile ids.
func (sm *StorageMetadata) RevertMap() map[hash.Hash]hash.Hash {
	revertMap := make(map[hash.Hash]hash.Hash)
	for _, artifact := range sm.artifacts {
		if artifact.storageType == Archive {
			md := artifact.arcMetadata
			revertMap[artifact.id] = hash.Parse(md.originalTableFileId)
		}
	}
	return revertMap
}

func (sm *StorageMetadata) oldGenTableExists(id hash.Hash) (bool, error) {
	path := filepath.Join(sm.root, ".dolt", "noms", "oldgen", id.String())
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// GetStorageMetadata returns metadata about the local filesystem storage for a single database. The path given must be
// the path to DB directory - ie, containing the .dolt directory.
func GetStorageMetadata(path string) (StorageMetadata, error) {
	err := validateDir(path)
	if err != nil {
		return StorageMetadata{}, err
	}

	// TODO: new gen and journal information in storage metadata will be useful in the future.
	//	newGen := filepath.Join(path, ".dolt", "noms")
	//	newgenManifest := filepath.Join(newGen, "manifest")

	oldgen := filepath.Join(path, ".dolt", "noms", "oldgen")
	oldgenManifest := filepath.Join(oldgen, "manifest")

	// If there is not oldgen manifest, then GC has never been run. Which is fine. We just don't have any oldgen.
	if _, err := os.Stat(oldgenManifest); err != nil {
		return StorageMetadata{}, nil
	}

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

		// If the oldgen/name exists, it's not an archive. If it exists with a .darc suffix, then it's an archive.
		tfName := tableSpecInfo.GetName()
		fullPath := filepath.Join(oldgen, tfName)
		_, err := os.Stat(fullPath)
		if err == nil {
			// exists.  Not an archive.
			artifacts = append(artifacts, StorageArtifact{
				id:          hash.Parse(tfName),
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
					id:          hash.Parse(tfName),
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

	return StorageMetadata{path, artifacts}, nil
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
