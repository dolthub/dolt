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

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func validateRepoRequest(req repoRequest) error {
	if req.GetRepoPath() == "" && req.GetRepoId() == nil {
		return fmt.Errorf("expected repo_path or repo_id, got neither")
	} else if req.GetRepoPath() == "" {
		id := req.GetRepoId()
		if id == nil || id.Org == "" || id.RepoName == "" {
			return fmt.Errorf("expected repo_id.org and repo_id.repo_name, missing at least one")
		}
	}
	return nil
}

func validateChunkTableInfo(field string, ctis []*remotesapi.ChunkTableInfo) error {
	for i, cti := range ctis {
		if len(cti.Hash) != hash.ByteLen {
			return fmt.Errorf("expected %s[%d].Hash to be %d bytes long, was %d", field, i, hash.ByteLen, len(cti.Hash))
		}
		if cti.ChunkCount == 0 {
			return fmt.Errorf("expected %s[%d].ChunkCount to be non-zero", field, i)
		}
	}
	return nil
}

func validateHash(field string, h []byte) error {
	if len(h) != hash.ByteLen {
		return fmt.Errorf("expected %s hash to be %d bytes long, was %d", field, hash.ByteLen, len(h))
	}
	return nil
}

func validateHashes(field string, hashes [][]byte) error {
	for i, bs := range hashes {
		if len(bs) != hash.ByteLen {
			return fmt.Errorf("expected %s[%d] hash to be %d bytes long, was %d", field, i, hash.ByteLen, len(bs))
		}
	}
	return nil
}

func ValidateGetRepoMetadataRequest(req *remotesapi.GetRepoMetadataRequest) error {
	if err := validateRepoRequest(req); err != nil {
		return err
	}
	if req.ClientRepoFormat == nil {
		return fmt.Errorf("expected non-nil client_repo_format")
	}
	if _, err := types.GetFormatForVersionString(req.ClientRepoFormat.NbfVersion); err != nil {
		return fmt.Errorf("unsupported value for client_repo_format.nbf_version: %w", err)
	}
	if req.ClientRepoFormat.NbsVersion != "4" && req.ClientRepoFormat.NbsVersion != "5" {
		return fmt.Errorf("unsupported value for client_repo_format.nbs_version: %v; expected \"4\" or \"5\"", req.ClientRepoFormat.NbsVersion)
	}
	return nil
}

func ValidateHasChunksRequest(req *remotesapi.HasChunksRequest) error {
	if err := validateRepoRequest(req); err != nil {
		return err
	}
	return validateHashes("hashes", req.Hashes)
}

func ValidateGetDownloadLocsRequest(req *remotesapi.GetDownloadLocsRequest) error {
	if err := validateRepoRequest(req); err != nil {
		return err
	}
	return validateHashes("chunk_hashes", req.ChunkHashes)
}

func ValidateGetUploadLocsRequest(req *remotesapi.GetUploadLocsRequest) error {
	if err := validateRepoRequest(req); err != nil {
		return err
	}
	return validateHashes("table_file_hashes", req.TableFileHashes)
}

func ValidateRebaseRequest(req *remotesapi.RebaseRequest) error {
	if err := validateRepoRequest(req); err != nil {
		return err
	}
	return nil
}

func ValidateRootRequest(req *remotesapi.RootRequest) error {
	if err := validateRepoRequest(req); err != nil {
		return err
	}
	return nil
}

func ValidateCommitRequest(req *remotesapi.CommitRequest) error {
	if err := validateRepoRequest(req); err != nil {
		return err
	}
	if err := validateHash("current", req.Current); err != nil {
		return err
	}
	if err := validateHash("last", req.Last); err != nil {
		return err
	}
	if err := validateChunkTableInfo("chunk_table_info", req.ChunkTableInfo); err != nil {
		return err
	}
	return nil
}

func ValidateListTableFilesRequest(req *remotesapi.ListTableFilesRequest) error {
	if err := validateRepoRequest(req); err != nil {
		return err
	}
	return nil
}

func ValidateRefreshTableFileUrlRequest(req *remotesapi.RefreshTableFileUrlRequest) error {
	if err := validateRepoRequest(req); err != nil {
		return err
	}
	if req.FileId == "" {
		return fmt.Errorf("expected file_id")
	}
	return nil
}

func ValidateAddTableFilesRequest(req *remotesapi.AddTableFilesRequest) error {
	if err := validateRepoRequest(req); err != nil {
		return err
	}
	if err := validateChunkTableInfo("chunk_table_info", req.ChunkTableInfo); err != nil {
		return err
	}
	return nil
}
