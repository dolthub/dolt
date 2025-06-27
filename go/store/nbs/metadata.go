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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
)

type TableFileMetadata struct {
	snappyChunkCount int
	snappyBytes      uint64
}

func (tfm *TableFileMetadata) SummaryString() string {
	sb := strings.Builder{}

	sb.WriteString("  Table File Metadata:\n")
	sb.WriteString(fmt.Sprintf("    Snappy Chunk Count: %d (bytes: %d)\n", tfm.snappyChunkCount, tfm.snappyBytes))

	return sb.String()
}

type ArchiveMetadata struct {
	originalTableFileId string
	formatVersion       int
	snappyChunkCount    int
	snappyBytes         uint64
	zStdChunkCount      int
	zStdBytes           uint64
	dictionaryCount     int
	dictionaryBytes     uint64
}

func (am *ArchiveMetadata) SummaryString() string {
	sb := strings.Builder{}

	sb.WriteString("  Archive Metadata:\n")
	sb.WriteString(fmt.Sprintf("    Format Version: %d\n", am.formatVersion))
	sb.WriteString(fmt.Sprintf("    Snappy Chunk Count: %d (bytes: %d)\n", am.snappyChunkCount, am.snappyBytes))
	sb.WriteString(fmt.Sprintf("    ZStd Chunk Count: %d (bytes: %d)\n", am.zStdChunkCount, am.zStdBytes))
	sb.WriteString(fmt.Sprintf("    Dictionary Count: %d (bytes: %d)\n", am.dictionaryCount, am.dictionaryBytes))

	return sb.String()
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
	// tblMetadata is additional metadata for table files. it is only set for storageType == TypeNoms.
	tblMetadata *TableFileMetadata
}

func (sa StorageArtifact) SummaryString() string {
	sb := strings.Builder{}

	sb.WriteString("Storage Artifact:\n")
	sb.WriteString("  ID: " + sa.id.String() + "\n")
	sb.WriteString("  Path: " + sa.path + "\n")

	if sa.storageType == TypeArchive {
		sb.WriteString(sa.arcMetadata.SummaryString())
	} else {
		sb.WriteString(sa.tblMetadata.SummaryString())
	}

	return sb.String()
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

func (sm *StorageMetadata) GetArtifacts() []StorageArtifact {
	return sm.artifacts
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

// GetStorageMetadata returns metadata about the local filesystem storage for a single database. The path given must be
// the path to DB directory - ie, containing the .dolt directory.
func GetStorageMetadata(ctx context.Context, path string, stats *Stats) (StorageMetadata, error) {
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
		artifact, err := buildArtifact(ctx, tableSpecInfo, newGen, stats)
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

		artifact, err := buildArtifact(ctx, tableSpecInfo, oldgen, stats)
		if err != nil {
			return StorageMetadata{}, err
		}
		artifacts = append(artifacts, artifact)
	}

	return StorageMetadata{path, artifacts}, nil
}

func buildArtifact(ctx context.Context, info TableSpecInfo, genPath string, stats *Stats) (StorageArtifact, error) {
	tfName := info.GetName()

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
		tblMeta, err := newTableFileMetadata(fullPath, info.GetChunkCount())
		if err != nil {
			return StorageArtifact{}, err
		}

		return StorageArtifact{
			id:          hash.Parse(tfName),
			path:        fullPath,
			storageType: TypeNoms,
			tblMetadata: tblMeta,
		}, nil
	} else {
		fra, err := newFileReaderAt(fullPath)
		if err != nil {
			return StorageArtifact{}, err
		}

		arcMetadata, err := newArchiveMetadata(ctx, fra, uint64(fra.sz), stats)
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
