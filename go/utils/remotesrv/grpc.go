package main

import (
	"context"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/gen/proto/dolt/services/remotesapi_v1alpha1"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/remotestorage"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
	"github.com/liquidata-inc/ld/dolt/go/store/go/hash"
	"github.com/liquidata-inc/ld/dolt/go/store/go/nbs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"sync/atomic"
)

type RemoteChunkStore struct {
	HttpHost string
	csCache  *DBCache
	bucket   string
}

func NewHttpFSBackedChunkStore(httpHost string, csCache *DBCache) *RemoteChunkStore {
	return &RemoteChunkStore{
		httpHost,
		csCache,
		"",
	}
}

func (rs RemoteChunkStore) HasChunks(ctx context.Context, req *remotesapi.HasChunksRequest) (*remotesapi.HasChunksResponse, error) {
	logger := getReqLogger("GRPC", "HasChunks")
	defer func() { logger("finished") }()

	cs := rs.getStore(req.RepoId, "HasChunks")

	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	logger(fmt.Sprintf("found repo %s/%s", req.RepoId.Org, req.RepoId.RepoName))

	hashes, hashToIndex := remotestorage.ParseByteSlices(req.Hashes)

	absent := cs.HasMany(ctx, hashes)
	indices := make([]int32, len(absent))

	logger(fmt.Sprintf("missing chunks: %v", indices))
	n := 0
	for h := range absent {
		indices[n] = int32(hashToIndex[h])
		n++
	}

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
	hashes, _ := remotestorage.ParseByteSlices(req.Hashes)
	locations := cs.GetChunkLocations(hashes)

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
		}

		logger("The URL is " + url)

		getRange := &remotesapi.HttpGetRange{Url: url, Ranges: ranges}
		locs = append(locs, &remotesapi.DownloadLoc{Location: &remotesapi.DownloadLoc_HttpGetRange{HttpGetRange: getRange}})
	}

	return &remotesapi.GetDownloadLocsResponse{Locs: locs}, nil
}

func (rs *RemoteChunkStore) getDownloadUrl(logger func(string), org, repoName, fileId string) (string, error) {
	return fmt.Sprintf("http://%s/%s/%s/%s", rs.HttpHost, org, repoName, fileId), nil
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
	hashes, _ := remotestorage.ParseByteSlices(req.Hashes)
	absent := cs.HasMany(ctx, hashes)

	var locs []*remotesapi.UploadLoc
	for h := range hashes {
		// if it's absent send the upload location
		if _, ok := absent[h]; ok {
			tmp := h
			url, err := rs.getUploadUrl(logger, org, repoName, h.String())

			if err != nil {
				return nil, status.Error(codes.Internal, "Failed to get upload Url.")
			}

			loc := &remotesapi.UploadLoc_HttpPost{HttpPost: &remotesapi.HttpPostChunk{Url: url}}
			locs = append(locs, &remotesapi.UploadLoc{Hash: tmp[:], Location: loc})

			logger(fmt.Sprintf("sending upload location for chunk %s: %s", h.String(), url))
		}
	}

	return &remotesapi.GetUploadLocsResponse{Locs: locs}, nil
}

func (rs *RemoteChunkStore) getUploadUrl(logger func(string), org, repoName, fileId string) (string, error) {
	return fmt.Sprintf("http://%s/%s/%s/%s", rs.HttpHost, org, repoName, fileId), nil
}

func (rs *RemoteChunkStore) Rebase(ctx context.Context, req *remotesapi.RebaseRequest) (*remotesapi.RebaseResponse, error) {
	logger := getReqLogger("GRPC", "Rebase")
	defer func() { logger("finished") }()

	cs := rs.getStore(req.RepoId, "Rebase")

	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	logger(fmt.Sprintf("found %s/%s", req.RepoId.Org, req.RepoId.RepoName))

	err := pantoerr.PanicToError("Rebase failed", func() error {
		cs.Rebase(ctx)
		return nil
	})

	if err != nil {
		cause := pantoerr.GetRecoveredPanicCause(err).(error)
		logger(fmt.Sprintf("panic occurred during processing of Rebace rpc of %s/%s details: %v", req.RepoId.Org, req.RepoId.RepoName, cause))
		return nil, status.Error(codes.Internal, "Failed to rebase")
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

	var h hash.Hash
	err := pantoerr.PanicToError("Root failed", func() error {
		h = cs.Root(ctx)
		return nil
	})

	if err != nil {
		cause := pantoerr.GetRecoveredPanicCause(err)
		logger(fmt.Sprintf("panic occurred during processing of Root rpc of %s/%s details: %v", req.RepoId.Org, req.RepoId.RepoName, cause))
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

	cs.UpdateManifest(ctx, updates)

	currHash := hash.New(req.Current)
	lastHash := hash.New(req.Last)

	var ok bool
	err := pantoerr.PanicToError("Commit failed", func() error {
		ok = cs.Commit(ctx, currHash, lastHash)
		return nil
	})

	if err != nil {
		cause := pantoerr.GetRecoveredPanicCause(err)
		logger(fmt.Sprintf("panic occurred during processing of Commit of %s/%s last %s curr: %s details: %v", req.RepoId.Org, req.RepoId.RepoName, lastHash.String(), currHash.String(), cause))
		return nil, status.Error(codes.Internal, "Failed to rebase")
	}

	logger(fmt.Sprintf("committed %s/%s moved from %s -> %s", req.RepoId.Org, req.RepoId.RepoName, currHash.String(), lastHash.String()))
	return &remotesapi.CommitResponse{Success: ok}, nil
}

func (rs *RemoteChunkStore) getStore(repoId *remotesapi.RepoId, rpcName string) *nbs.NomsBlockStore {
	org := repoId.Org
	repoName := repoId.RepoName

	cs, err := rs.csCache.Get(org, repoName)

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
