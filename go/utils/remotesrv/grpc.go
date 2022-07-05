// Copyright 2019 Dolthub, Inc.
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

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

type RemoteChunkStore struct {
	HttpHost string
	csCache  *DBCache
	bucket   string
	remotesapi.UnimplementedChunkStoreServiceServer
}

func NewHttpFSBackedChunkStore(httpHost string, csCache *DBCache) *RemoteChunkStore {
	return &RemoteChunkStore{
		HttpHost: httpHost,
		csCache:  csCache,
		bucket:   "",
	}
}

func (rs *RemoteChunkStore) HasChunks(ctx context.Context, req *remotesapi.HasChunksRequest) (*remotesapi.HasChunksResponse, error) {
	logger := getReqLogger("GRPC", "HasChunks")
	defer func() { logger("finished") }()

	cs := rs.getStore(req.RepoId, "HasChunks")

	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	logger(fmt.Sprintf("found repo %s/%s", req.RepoId.Org, req.RepoId.RepoName))

	hashes, hashToIndex := remotestorage.ParseByteSlices(req.Hashes)

	absent, err := cs.HasMany(ctx, hashes)

	if err != nil {
		return nil, status.Error(codes.Internal, "HasMany failure:"+err.Error())
	}

	indices := make([]int32, len(absent))

	n := 0
	for h := range absent {
		indices[n] = int32(hashToIndex[h])
		n++
	}

	//logger(fmt.Sprintf("missing chunks: %v", indices))

	resp := &remotesapi.HasChunksResponse{
		Absent: indices,
	}

	return resp, nil
}

func (rs *RemoteChunkStore) GetDownloadLocations(ctx context.Context, req *remotesapi.GetDownloadLocsRequest) (*remotesapi.GetDownloadLocsResponse, error) {
	logger := getReqLogger("GRPC", "GetDownloadLocations")
	defer func() { logger("finished") }()

	cs := rs.getStore(req.RepoId, "GetDownloadLoctions")

	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	logger(fmt.Sprintf("found repo %s/%s", req.RepoId.Org, req.RepoId.RepoName))

	org := req.RepoId.Org
	repoName := req.RepoId.RepoName
	hashes, _ := remotestorage.ParseByteSlices(req.ChunkHashes)
	locations, err := cs.GetChunkLocations(hashes)

	if err != nil {
		return nil, err
	}

	var locs []*remotesapi.DownloadLoc
	for loc, hashToRange := range locations {
		var ranges []*remotesapi.RangeChunk
		for h, r := range hashToRange {
			hCpy := h
			ranges = append(ranges, &remotesapi.RangeChunk{Hash: hCpy[:], Offset: r.Offset, Length: r.Length})
		}

		url, err := rs.getDownloadUrl(logger, org, repoName, loc.String())
		if err != nil {
			log.Println("Failed to sign request", err)
			return nil, err
		}

		logger("The URL is " + url)

		getRange := &remotesapi.HttpGetRange{Url: url, Ranges: ranges}
		locs = append(locs, &remotesapi.DownloadLoc{Location: &remotesapi.DownloadLoc_HttpGetRange{HttpGetRange: getRange}})
	}

	return &remotesapi.GetDownloadLocsResponse{Locs: locs}, nil
}

func (rs *RemoteChunkStore) StreamDownloadLocations(stream remotesapi.ChunkStoreService_StreamDownloadLocationsServer) error {
	logger := getReqLogger("GRPC", "StreamDownloadLocations")
	defer func() { logger("finished") }()

	var repoID *remotesapi.RepoId
	var cs *nbs.NomsBlockStore
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if !proto.Equal(req.RepoId, repoID) {
			repoID = req.RepoId
			cs = rs.getStore(repoID, "StreamDownloadLoctions")
			if cs == nil {
				return status.Error(codes.Internal, "Could not get chunkstore")
			}
			logger(fmt.Sprintf("found repo %s/%s", repoID.Org, repoID.RepoName))
		}

		org := req.RepoId.Org
		repoName := req.RepoId.RepoName
		hashes, _ := remotestorage.ParseByteSlices(req.ChunkHashes)
		locations, err := cs.GetChunkLocations(hashes)
		if err != nil {
			return err
		}

		var locs []*remotesapi.DownloadLoc
		for loc, hashToRange := range locations {
			var ranges []*remotesapi.RangeChunk
			for h, r := range hashToRange {
				hCpy := h
				ranges = append(ranges, &remotesapi.RangeChunk{Hash: hCpy[:], Offset: r.Offset, Length: r.Length})
			}

			url, err := rs.getDownloadUrl(logger, org, repoName, loc.String())
			if err != nil {
				log.Println("Failed to sign request", err)
				return err
			}

			logger("The URL is " + url)

			getRange := &remotesapi.HttpGetRange{Url: url, Ranges: ranges}
			locs = append(locs, &remotesapi.DownloadLoc{Location: &remotesapi.DownloadLoc_HttpGetRange{HttpGetRange: getRange}})
		}

		if err := stream.Send(&remotesapi.GetDownloadLocsResponse{Locs: locs}); err != nil {
			return err
		}
	}
}

func (rs *RemoteChunkStore) getDownloadUrl(logger func(string), org, repoName, fileId string) (string, error) {
	return fmt.Sprintf("http://%s/%s/%s/%s", rs.HttpHost, org, repoName, fileId), nil
}

func parseTableFileDetails(req *remotesapi.GetUploadLocsRequest) []*remotesapi.TableFileDetails {
	tfd := req.GetTableFileDetails()

	if len(tfd) == 0 {
		_, hashToIdx := remotestorage.ParseByteSlices(req.TableFileHashes)

		tfd = make([]*remotesapi.TableFileDetails, len(hashToIdx))
		for h, i := range hashToIdx {
			tfd[i] = &remotesapi.TableFileDetails{
				Id:            h[:],
				ContentLength: 0,
				ContentHash:   nil,
			}
		}
	}

	return tfd
}

func (rs *RemoteChunkStore) GetUploadLocations(ctx context.Context, req *remotesapi.GetUploadLocsRequest) (*remotesapi.GetUploadLocsResponse, error) {
	logger := getReqLogger("GRPC", "GetUploadLocations")
	defer func() { logger("finished") }()

	cs := rs.getStore(req.RepoId, "GetWriteChunkUrls")

	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	logger(fmt.Sprintf("found repo %s/%s", req.RepoId.Org, req.RepoId.RepoName))

	org := req.RepoId.Org
	repoName := req.RepoId.RepoName
	tfds := parseTableFileDetails(req)

	var locs []*remotesapi.UploadLoc
	for _, tfd := range tfds {
		h := hash.New(tfd.Id)
		url, err := rs.getUploadUrl(logger, org, repoName, tfd)

		if err != nil {
			return nil, status.Error(codes.Internal, "Failed to get upload Url.")
		}

		loc := &remotesapi.UploadLoc_HttpPost{HttpPost: &remotesapi.HttpPostTableFile{Url: url}}
		locs = append(locs, &remotesapi.UploadLoc{TableFileHash: h[:], Location: loc})

		logger(fmt.Sprintf("sending upload location for chunk %s: %s", h.String(), url))
	}

	return &remotesapi.GetUploadLocsResponse{Locs: locs}, nil
}

func (rs *RemoteChunkStore) getUploadUrl(logger func(string), org, repoName string, tfd *remotesapi.TableFileDetails) (string, error) {
	fileID := hash.New(tfd.Id).String()
	expectedFiles[fileID] = tfd
	return fmt.Sprintf("http://%s/%s/%s/%s", rs.HttpHost, org, repoName, fileID), nil
}

func (rs *RemoteChunkStore) Rebase(ctx context.Context, req *remotesapi.RebaseRequest) (*remotesapi.RebaseResponse, error) {
	logger := getReqLogger("GRPC", "Rebase")
	defer func() { logger("finished") }()

	cs := rs.getStore(req.RepoId, "Rebase")

	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	logger(fmt.Sprintf("found %s/%s", req.RepoId.Org, req.RepoId.RepoName))

	err := cs.Rebase(ctx)

	if err != nil {
		logger(fmt.Sprintf("error occurred during processing of Rebace rpc of %s/%s details: %v", req.RepoId.Org, req.RepoId.RepoName, err))
		return nil, status.Errorf(codes.Internal, "failed to rebase: %v", err)
	}

	return &remotesapi.RebaseResponse{}, nil
}

func (rs *RemoteChunkStore) Root(ctx context.Context, req *remotesapi.RootRequest) (*remotesapi.RootResponse, error) {
	logger := getReqLogger("GRPC", "Root")
	defer func() { logger("finished") }()

	cs := rs.getStore(req.RepoId, "Root")

	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	h, err := cs.Root(ctx)

	if err != nil {
		logger(fmt.Sprintf("error occurred during processing of Root rpc of %s/%s details: %v", req.RepoId.Org, req.RepoId.RepoName, err))
		return nil, status.Error(codes.Internal, "Failed to get root")
	}

	return &remotesapi.RootResponse{RootHash: h[:]}, nil
}

func (rs *RemoteChunkStore) Commit(ctx context.Context, req *remotesapi.CommitRequest) (*remotesapi.CommitResponse, error) {
	logger := getReqLogger("GRPC", "Commit")
	defer func() { logger("finished") }()

	cs := rs.getStore(req.RepoId, "Commit")

	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	logger(fmt.Sprintf("found %s/%s", req.RepoId.Org, req.RepoId.RepoName))

	//should validate
	updates := make(map[hash.Hash]uint32)
	for _, cti := range req.ChunkTableInfo {
		updates[hash.New(cti.Hash)] = cti.ChunkCount
	}

	_, err := cs.UpdateManifest(ctx, updates)

	if err != nil {
		logger(fmt.Sprintf("error occurred updating the manifest: %s", err.Error()))
		return nil, status.Errorf(codes.Internal, "manifest update error: %v", err)
	}

	currHash := hash.New(req.Current)
	lastHash := hash.New(req.Last)

	var ok bool
	ok, err = cs.Commit(ctx, currHash, lastHash)

	if err != nil {
		logger(fmt.Sprintf("error occurred during processing of Commit of %s/%s last %s curr: %s details: %v", req.RepoId.Org, req.RepoId.RepoName, lastHash.String(), currHash.String(), err))
		return nil, status.Errorf(codes.Internal, "failed to rebase: %v", err)
	}

	logger(fmt.Sprintf("committed %s/%s moved from %s -> %s", req.RepoId.Org, req.RepoId.RepoName, currHash.String(), lastHash.String()))
	return &remotesapi.CommitResponse{Success: ok}, nil
}

func (rs *RemoteChunkStore) GetRepoMetadata(ctx context.Context, req *remotesapi.GetRepoMetadataRequest) (*remotesapi.GetRepoMetadataResponse, error) {
	logger := getReqLogger("GRPC", "GetRepoMetadata")
	defer func() { logger("finished") }()

	cs := rs.getOrCreateStore(req.RepoId, "GetRepoMetadata", req.ClientRepoFormat.NbfVersion)
	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	_, tfs, _, err := cs.Sources(ctx)

	if err != nil {
		return nil, err
	}

	var size uint64
	for _, tf := range tfs {
		path := filepath.Join(req.RepoId.Org, req.RepoId.RepoName, tf.FileID())
		info, err := os.Stat(path)

		if err != nil {
			return nil, err
		}

		size += uint64(info.Size())
	}

	return &remotesapi.GetRepoMetadataResponse{
		NbfVersion:  cs.Version(),
		NbsVersion:  req.ClientRepoFormat.NbsVersion,
		StorageSize: size,
	}, nil
}

func (rs *RemoteChunkStore) ListTableFiles(ctx context.Context, req *remotesapi.ListTableFilesRequest) (*remotesapi.ListTableFilesResponse, error) {
	logger := getReqLogger("GRPC", "ListTableFiles")
	defer func() { logger("finished") }()

	cs := rs.getStore(req.RepoId, "ListTableFiles")

	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	logger(fmt.Sprintf("found repo %s/%s", req.RepoId.Org, req.RepoId.RepoName))

	root, tables, appendixTables, err := cs.Sources(ctx)

	if err != nil {
		return nil, status.Error(codes.Internal, "failed to get sources")
	}

	tableFileInfo, err := getTableFileInfo(rs, logger, tables, req)
	if err != nil {
		return nil, err
	}

	appendixTableFileInfo, err := getTableFileInfo(rs, logger, appendixTables, req)
	if err != nil {
		return nil, err
	}

	resp := &remotesapi.ListTableFilesResponse{
		RootHash:              root[:],
		TableFileInfo:         tableFileInfo,
		AppendixTableFileInfo: appendixTableFileInfo,
	}

	return resp, nil
}

func getTableFileInfo(rs *RemoteChunkStore, logger func(string), tableList []nbs.TableFile, req *remotesapi.ListTableFilesRequest) ([]*remotesapi.TableFileInfo, error) {
	appendixTableFileInfo := make([]*remotesapi.TableFileInfo, 0)
	for _, t := range tableList {
		url, err := rs.getDownloadUrl(logger, req.RepoId.Org, req.RepoId.RepoName, t.FileID())
		if err != nil {
			return nil, status.Error(codes.Internal, "failed to get download url for "+t.FileID())
		}

		appendixTableFileInfo = append(appendixTableFileInfo, &remotesapi.TableFileInfo{
			FileId:    t.FileID(),
			NumChunks: uint32(t.NumChunks()),
			Url:       url,
		})
	}
	return appendixTableFileInfo, nil
}

// AddTableFiles updates the remote manifest with new table files without modifying the root hash.
func (rs *RemoteChunkStore) AddTableFiles(ctx context.Context, req *remotesapi.AddTableFilesRequest) (*remotesapi.AddTableFilesResponse, error) {
	logger := getReqLogger("GRPC", "Commit")
	defer func() { logger("finished") }()

	cs := rs.getStore(req.RepoId, "Commit")

	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	logger(fmt.Sprintf("found %s/%s", req.RepoId.Org, req.RepoId.RepoName))

	// should validate
	updates := make(map[hash.Hash]uint32)
	for _, cti := range req.ChunkTableInfo {
		updates[hash.New(cti.Hash)] = cti.ChunkCount
	}

	_, err := cs.UpdateManifest(ctx, updates)

	if err != nil {
		logger(fmt.Sprintf("error occurred updating the manifest: %s", err.Error()))
		return nil, status.Error(codes.Internal, "manifest update error")
	}

	return &remotesapi.AddTableFilesResponse{Success: true}, nil
}

func (rs *RemoteChunkStore) getStore(repoId *remotesapi.RepoId, rpcName string) *nbs.NomsBlockStore {
	return rs.getOrCreateStore(repoId, rpcName, types.Format_Default.VersionString())
}

func (rs *RemoteChunkStore) getOrCreateStore(repoId *remotesapi.RepoId, rpcName, nbfVerStr string) *nbs.NomsBlockStore {
	org := repoId.Org
	repoName := repoId.RepoName

	cs, err := rs.csCache.Get(org, repoName, nbfVerStr)

	if err != nil {
		log.Printf("Failed to retrieve chunkstore for %s/%s\n", org, repoName)
	}

	return cs
}

var requestId int32

func incReqId() int32 {
	return atomic.AddInt32(&requestId, 1)
}

func getReqLogger(method, callName string) func(string) {
	callId := fmt.Sprintf("%s(%05d)", method, incReqId())
	log.Println(callId, "new request for:", callName)

	return func(msg string) {
		log.Println(callId, "-", msg)
	}
}
