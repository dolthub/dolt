// Copyright 2023 Dolthub, Inc.
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

package remotesrv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/store/hash"
)

var GoodRepoPath = "dolthub/database"
var GoodRepoId = &remotesapi.RepoId{
	Org:      "dolthub",
	RepoName: "database",
}

var GoodHash = make([]byte, hash.ByteLen)
var ShortHash = make([]byte, hash.ByteLen-1)
var LongHash = make([]byte, hash.ByteLen+1)

func TestValidateGetRepoMetadataRequest(t *testing.T) {
	for i, errMsg := range []*remotesapi.GetRepoMetadataRequest{
		{},
		{
			RepoPath: GoodRepoPath,
			ClientRepoFormat: &remotesapi.ClientRepoFormat{
				NbfVersion: "__DOLT__",
			},
		},
		{
			RepoPath: GoodRepoPath,
			ClientRepoFormat: &remotesapi.ClientRepoFormat{
				NbfVersion: "__DOLT__",
				NbsVersion: "UNKNOWN",
			},
		},
		{
			RepoPath: GoodRepoPath,
			ClientRepoFormat: &remotesapi.ClientRepoFormat{
				NbsVersion: "5",
			},
		},
		{
			RepoPath: GoodRepoPath,
			ClientRepoFormat: &remotesapi.ClientRepoFormat{
				NbfVersion: "__UNKNOWN__",
				NbsVersion: "5",
			},
		},
		{
			RepoId: &remotesapi.RepoId{},
			ClientRepoFormat: &remotesapi.ClientRepoFormat{
				NbfVersion: "__DOLT__",
				NbsVersion: "5",
			},
		},
		{
			RepoId: &remotesapi.RepoId{
				Org: "dolthub",
			},
			ClientRepoFormat: &remotesapi.ClientRepoFormat{
				NbfVersion: "__DOLT__",
				NbsVersion: "5",
			},
		},
		{
			RepoId: &remotesapi.RepoId{
				RepoName: "database",
			},
			ClientRepoFormat: &remotesapi.ClientRepoFormat{
				NbfVersion: "__DOLT__",
				NbsVersion: "5",
			},
		},
	} {
		t.Run(fmt.Sprintf("Error #%02d", i), func(t *testing.T) {
			assert.Error(t, ValidateGetRepoMetadataRequest(errMsg), "%v should not validate", errMsg)
		})
	}
	for i, msg := range []*remotesapi.GetRepoMetadataRequest{
		{
			RepoPath: GoodRepoPath,
			ClientRepoFormat: &remotesapi.ClientRepoFormat{
				NbfVersion: "__DOLT__",
				NbsVersion: "5",
			},
		},
		{
			RepoId: GoodRepoId,
			ClientRepoFormat: &remotesapi.ClientRepoFormat{
				NbfVersion: "__DOLT__",
				NbsVersion: "5",
			},
		},
		{
			RepoPath: GoodRepoPath,
			ClientRepoFormat: &remotesapi.ClientRepoFormat{
				NbfVersion: "__DOLT__",
				NbsVersion: "4",
			},
		},
		{
			RepoPath: GoodRepoPath,
			ClientRepoFormat: &remotesapi.ClientRepoFormat{
				NbfVersion: "__LD_1__",
				NbsVersion: "5",
			},
		},
	} {
		t.Run(fmt.Sprintf("NoError #%02d", i), func(t *testing.T) {
			assert.NoError(t, ValidateGetRepoMetadataRequest(msg), "%v should validate", msg)
		})
	}
}

func TestValidateHasChunksRequest(t *testing.T) {
	for i, errMsg := range []*remotesapi.HasChunksRequest{
		{},
		{
			RepoId: &remotesapi.RepoId{
				Org: "dolthub",
			},
			Hashes: [][]byte{GoodHash},
		},
		{
			RepoId: &remotesapi.RepoId{
				RepoName: "database",
			},
			Hashes: [][]byte{GoodHash},
		},
		{
			RepoPath: GoodRepoPath,
			Hashes:   [][]byte{ShortHash},
		},
		{
			RepoPath: GoodRepoPath,
			Hashes:   [][]byte{LongHash},
		},
		{
			RepoPath: GoodRepoPath,
			Hashes:   [][]byte{GoodHash, GoodHash, LongHash, GoodHash},
		},
	} {
		t.Run(fmt.Sprintf("Error #%02d", i), func(t *testing.T) {
			assert.Error(t, ValidateHasChunksRequest(errMsg), "%v should not validate", errMsg)
		})
	}
	for i, msg := range []*remotesapi.HasChunksRequest{
		{
			RepoPath: GoodRepoPath,
		},
		{
			RepoId: GoodRepoId,
		},
		{
			RepoPath: GoodRepoPath,
			Hashes:   [][]byte{GoodHash},
		},
	} {
		t.Run(fmt.Sprintf("NoError #%02d", i), func(t *testing.T) {
			assert.NoError(t, ValidateHasChunksRequest(msg), "%v should validate", msg)
		})
	}
}

func TestValidateGetDownloadLocsRequest(t *testing.T) {
	for i, errMsg := range []*remotesapi.GetDownloadLocsRequest{
		{},
		{
			RepoId: &remotesapi.RepoId{
				Org: "dolthub",
			},
			ChunkHashes: [][]byte{GoodHash},
		},
		{
			RepoId: &remotesapi.RepoId{
				RepoName: "database",
			},
			ChunkHashes: [][]byte{GoodHash},
		},
		{
			RepoPath:    GoodRepoPath,
			ChunkHashes: [][]byte{ShortHash},
		},
		{
			RepoPath:    GoodRepoPath,
			ChunkHashes: [][]byte{LongHash},
		},
		{
			RepoPath:    GoodRepoPath,
			ChunkHashes: [][]byte{GoodHash, GoodHash, LongHash, GoodHash},
		},
	} {
		t.Run(fmt.Sprintf("Error #%02d", i), func(t *testing.T) {
			assert.Error(t, ValidateGetDownloadLocsRequest(errMsg), "%v should not validate", errMsg)
		})
	}
	for i, msg := range []*remotesapi.GetDownloadLocsRequest{
		{
			RepoPath: GoodRepoPath,
		},
		{
			RepoId: GoodRepoId,
		},
		{
			RepoPath:    GoodRepoPath,
			ChunkHashes: [][]byte{GoodHash},
		},
	} {
		t.Run(fmt.Sprintf("NoError #%02d", i), func(t *testing.T) {
			assert.NoError(t, ValidateGetDownloadLocsRequest(msg), "%v should validate", msg)
		})
	}
}

func TestValidateGetUploadLocsRequest(t *testing.T) {
	for i, errMsg := range []*remotesapi.GetUploadLocsRequest{
		{},
		{
			RepoId: &remotesapi.RepoId{
				Org: "dolthub",
			},
			TableFileHashes: [][]byte{GoodHash},
		},
		{
			RepoId: &remotesapi.RepoId{
				RepoName: "database",
			},
			TableFileHashes: [][]byte{GoodHash},
		},
		{
			RepoPath:        GoodRepoPath,
			TableFileHashes: [][]byte{ShortHash},
		},
		{
			RepoPath:        GoodRepoPath,
			TableFileHashes: [][]byte{LongHash},
		},
		{
			RepoPath:        GoodRepoPath,
			TableFileHashes: [][]byte{GoodHash, GoodHash, LongHash, GoodHash},
		},
	} {
		t.Run(fmt.Sprintf("Error #%02d", i), func(t *testing.T) {
			assert.Error(t, ValidateGetUploadLocsRequest(errMsg), "%v should not validate", errMsg)
		})
	}
	for i, msg := range []*remotesapi.GetUploadLocsRequest{
		{
			RepoPath: GoodRepoPath,
		},
		{
			RepoId: GoodRepoId,
		},
		{
			RepoPath:        GoodRepoPath,
			TableFileHashes: [][]byte{GoodHash},
		},
	} {
		t.Run(fmt.Sprintf("NoError #%02d", i), func(t *testing.T) {
			assert.NoError(t, ValidateGetUploadLocsRequest(msg), "%v should validate", msg)
		})
	}
}

func TestValidateRebaseRequest(t *testing.T) {
	for i, errMsg := range []*remotesapi.RebaseRequest{
		{},
		{
			RepoId: &remotesapi.RepoId{
				Org: "dolthub",
			},
		},
		{
			RepoId: &remotesapi.RepoId{
				RepoName: "database",
			},
		},
	} {
		t.Run(fmt.Sprintf("Error #%02d", i), func(t *testing.T) {
			assert.Error(t, ValidateRebaseRequest(errMsg), "%v should not validate", errMsg)
		})
	}
	for i, msg := range []*remotesapi.RebaseRequest{
		{
			RepoPath: GoodRepoPath,
		},
	} {
		t.Run(fmt.Sprintf("NoError #%02d", i), func(t *testing.T) {
			assert.NoError(t, ValidateRebaseRequest(msg), "%v should validate", msg)
		})
	}
}

func TestValidateRootRequest(t *testing.T) {
	for i, errMsg := range []*remotesapi.RootRequest{
		{},
		{
			RepoId: &remotesapi.RepoId{
				Org: "dolthub",
			},
		},
		{
			RepoId: &remotesapi.RepoId{
				RepoName: "database",
			},
		},
	} {
		t.Run(fmt.Sprintf("Error #%02d", i), func(t *testing.T) {
			assert.Error(t, ValidateRootRequest(errMsg), "%v should not validate", errMsg)
		})
	}
	for i, msg := range []*remotesapi.RootRequest{
		{
			RepoPath: GoodRepoPath,
		},
		{
			RepoId: GoodRepoId,
		},
	} {
		t.Run(fmt.Sprintf("NoError #%02d", i), func(t *testing.T) {
			assert.NoError(t, ValidateRootRequest(msg), "%v should validate", msg)
		})
	}
}

func TestValidateCommitRequest(t *testing.T) {
	for i, errMsg := range []*remotesapi.CommitRequest{
		{},
		{
			RepoPath: GoodRepoPath,
			Last:     GoodHash,
		},
		{
			RepoPath: GoodRepoPath,
			Current:  GoodHash,
		},
		{
			RepoId: &remotesapi.RepoId{
				Org: "dolthub",
			},
			Current: GoodHash,
			Last:    GoodHash,
		},
		{
			RepoId: &remotesapi.RepoId{
				RepoName: "database",
			},
			Current: GoodHash,
			Last:    GoodHash,
		},
		{
			RepoId:  GoodRepoId,
			Current: GoodHash,
			Last:    GoodHash,
			ChunkTableInfo: []*remotesapi.ChunkTableInfo{
				{
					Hash: GoodHash,
				},
			},
		},
		{
			RepoId:  GoodRepoId,
			Current: GoodHash,
			Last:    GoodHash,
			ChunkTableInfo: []*remotesapi.ChunkTableInfo{
				{
					Hash:       LongHash,
					ChunkCount: 32,
				},
			},
		},
	} {
		t.Run(fmt.Sprintf("Error #%02d", i), func(t *testing.T) {
			assert.Error(t, ValidateCommitRequest(errMsg), "%v should not validate", errMsg)
		})
	}
	for i, msg := range []*remotesapi.CommitRequest{
		{
			RepoPath: GoodRepoPath,
			Current:  GoodHash,
			Last:     GoodHash,
		},
		{
			RepoId:  GoodRepoId,
			Current: GoodHash,
			Last:    GoodHash,
		},
		{
			RepoId:  GoodRepoId,
			Current: GoodHash,
			Last:    GoodHash,
			ChunkTableInfo: []*remotesapi.ChunkTableInfo{
				{
					Hash:       GoodHash,
					ChunkCount: 32,
				},
			},
		},
	} {
		t.Run(fmt.Sprintf("NoError #%02d", i), func(t *testing.T) {
			assert.NoError(t, ValidateCommitRequest(msg), "%v should validate", msg)
		})
	}
}

func TestValidateListTableFilesRequest(t *testing.T) {
	for i, errMsg := range []*remotesapi.ListTableFilesRequest{
		{},
		{
			RepoId: &remotesapi.RepoId{
				Org: "dolthub",
			},
		},
		{
			RepoId: &remotesapi.RepoId{
				RepoName: "database",
			},
		},
	} {
		t.Run(fmt.Sprintf("Error #%02d", i), func(t *testing.T) {
			assert.Error(t, ValidateListTableFilesRequest(errMsg), "%v should not validate", errMsg)
		})
	}
	for i, msg := range []*remotesapi.ListTableFilesRequest{
		{
			RepoPath: GoodRepoPath,
		},
		{
			RepoId: GoodRepoId,
		},
	} {
		t.Run(fmt.Sprintf("NoError #%02d", i), func(t *testing.T) {
			assert.NoError(t, ValidateListTableFilesRequest(msg), "%v should validate", msg)
		})
	}
}

func TestValidateRefreshTableFileUrlRequest(t *testing.T) {
	for i, errMsg := range []*remotesapi.RefreshTableFileUrlRequest{
		{},
		{
			RepoId: &remotesapi.RepoId{
				Org: "dolthub",
			},
		},
		{
			RepoId: &remotesapi.RepoId{
				RepoName: "database",
			},
		},
	} {
		t.Run(fmt.Sprintf("Error #%02d", i), func(t *testing.T) {
			assert.Error(t, ValidateRefreshTableFileUrlRequest(errMsg), "%v should not validate", errMsg)
		})
	}
	for i, msg := range []*remotesapi.RefreshTableFileUrlRequest{} {
		t.Run(fmt.Sprintf("NoError #%02d", i), func(t *testing.T) {
			assert.NoError(t, ValidateRefreshTableFileUrlRequest(msg), "%v should validate", msg)
		})
	}
}

func TestValidateAddTableFilesRequest(t *testing.T) {
	for i, errMsg := range []*remotesapi.AddTableFilesRequest{
		{},
		{
			RepoId: &remotesapi.RepoId{
				Org: "dolthub",
			},
		},
		{
			RepoId: &remotesapi.RepoId{
				RepoName: "database",
			},
		},
		{
			RepoId: GoodRepoId,
			ChunkTableInfo: []*remotesapi.ChunkTableInfo{
				{
					Hash: GoodHash,
				},
			},
		},
		{
			RepoId: GoodRepoId,
			ChunkTableInfo: []*remotesapi.ChunkTableInfo{
				{
					Hash:       LongHash,
					ChunkCount: 32,
				},
			},
		},
	} {
		t.Run(fmt.Sprintf("Error #%02d", i), func(t *testing.T) {
			assert.Error(t, ValidateAddTableFilesRequest(errMsg), "%v should not validate", errMsg)
		})
	}
	for i, msg := range []*remotesapi.AddTableFilesRequest{
		{
			RepoPath: GoodRepoPath,
		},
		{
			RepoId: GoodRepoId,
		},
		{
			RepoId: GoodRepoId,
			ChunkTableInfo: []*remotesapi.ChunkTableInfo{
				{
					Hash:       GoodHash,
					ChunkCount: 32,
				},
			},
		},
	} {
		t.Run(fmt.Sprintf("NoError #%02d", i), func(t *testing.T) {
			assert.NoError(t, ValidateAddTableFilesRequest(msg), "%v should validate", msg)
		})
	}
}
