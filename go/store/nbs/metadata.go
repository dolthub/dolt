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
	"errors"
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
)

type ArchiveMetadata struct {
	originalTableFileId string
}

type TableFileFormat int

const (
	TypeNoms TableFileFormat = iota
	TypeArchive
)

type StorageArtifact struct {
	// ID of the storage artifact. This is uses in the manifest to identify the artifact, but it is not the file name.
	// as archives has a suffix.
	id hash.Hash
	// path to the storage artifact.
	path string
	// storageType is the type of the storage artifact.
	storageType TableFileFormat
	// arcMetadata is additional metadata for archive files. it is only set for storageType == TypeArchive.
	arcMetadata *ArchiveMetadata
}

type StorageMetadata struct {
	// root is the path to storage. Specifically, it contains a .dolt directory.
	root      string
	artifacts []StorageArtifact
}

func (sm *StorageMetadata) ArchiveFilesPresent() bool {
	for _, artifact := range sm.artifacts {
		if artifact.storageType == TypeArchive {
			return true
		}
	}
	return false
}

// RevertMap returns a map of Archive file ids to their origin TableFile ids.
func (sm *StorageMetadata) RevertMap() map[hash.Hash]hash.Hash {
	revertMap := make(map[hash.Hash]hash.Hash)
	for _, artifact := range sm.artifacts {
		if artifact.storageType == TypeArchive {
			md := artifact.arcMetadata
			revertMap[artifact.id] = hash.Parse(md.originalTableFileId)
		}
	}
	return revertMap
}

// oldGenTableExists returns true if the table file exists in the oldgen directory. This is a file system check for
// a table file we have no record of, but may be useful in the process of reverting an archive operation.
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

	newGen := filepath.Join(path, ".dolt", "noms")
	newgenManifest := filepath.Join(newGen, "manifest")
	manifestReader, err := os.Open(newgenManifest)
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
		artifact, err := buildArtifact(tableSpecInfo, newGen)
		if err != nil {
			return StorageMetadata{}, err
		}
		artifacts = append(artifacts, artifact)
	}

	oldgen := filepath.Join(newGen, "oldgen")
	oldgenManifest := filepath.Join(oldgen, "manifest")

	// If there is no oldgen manifest, then GC has never been run. Which is fine. We just don't have any oldgen.
	if _, err := os.Stat(oldgenManifest); err != nil {
		return StorageMetadata{path, artifacts}, nil
	}

	manifestReader, err = os.Open(oldgenManifest)
	if err != nil {
		return StorageMetadata{}, err
	}
	manifest, err = ParseManifest(manifestReader)
	if err != nil {
		return StorageMetadata{}, err
	}

	for i := 0; i < manifest.NumTableSpecs(); i++ {
		tableSpecInfo := manifest.GetTableSpecInfo(i)

		artifact, err := buildArtifact(tableSpecInfo, oldgen)
		if err != nil {
			return StorageMetadata{}, err
		}
		artifacts = append(artifacts, artifact)
	}

	return StorageMetadata{path, artifacts}, nil
}

func buildArtifact(info TableSpecInfo, genPath string) (StorageArtifact, error) {
	tfName := info.GetName()

	// This code is going to be removed as soon as backup supports archives.
	archive := false
	fullPath := filepath.Join(genPath, tfName)

	_, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fullPath = filepath.Join(genPath, tfName+ArchiveFileSuffix)
		} else {
			return StorageArtifact{}, err
		}
		_, err = os.Stat(fullPath)
		if err != nil {
			return StorageArtifact{}, err
		}
		archive = true
	}

	if !archive {
		return StorageArtifact{
			id:          hash.Parse(tfName),
			path:        fullPath,
			storageType: TypeNoms,
		}, nil
	} else {
		reader, fileSize, err := openReader(fullPath)
		if err != nil {
			return StorageArtifact{}, err
		}

		arcMetadata, err := newArchiveMetadata(reader, fileSize)
		if err != nil {
			return StorageArtifact{}, err
		}

		return StorageArtifact{
			id:          hash.Parse(tfName),
			path:        fullPath,
			storageType: TypeArchive,
			arcMetadata: arcMetadata,
		}, nil
	}
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
