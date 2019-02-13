package main

import (
	"context"
	"fmt"
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/gen/proto/dolt/services/remotesapi_v1alpha1"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/remotestorage"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
)

type RemoteChunkStore struct {
}

func (rs RemoteChunkStore) HasChunks(ctx context.Context, req *remotesapi.HasChunksRequest) (*remotesapi.HasChunksResponse, error) {
	logFinish := logStart("HasChunks")
	defer logFinish()

	cs := getStore(req.RepoId, "HasChunks")

	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	hashes, hashToIndex := remotestorage.ParseByteSlices(req.Hashes)

	absent := cs.HasMany(hashes)
	indices := make([]int32, len(absent))

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

func (rs RemoteChunkStore) GetDownloadLoctions(ctx context.Context, req *remotesapi.GetDownloadLocsRequest) (*remotesapi.GetDownloadLocsResponse, error) {
	logFinish := logStart("GetDownloadLoctions")
	defer logFinish()

	cs := getStore(req.RepoId, "GetDownloadLoctions")

	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	org := req.RepoId.Org
	repoName := req.RepoId.RepoName
	hashes, _ := remotestorage.ParseByteSlices(req.Hashes)
	absent := cs.HasMany(hashes)

	var locs []*remotesapi.DownloadLoc
	for h := range hashes {
		// if it's not absent send the download location
		if _, ok := absent[h]; !ok {
			tmp := h
			url := fmt.Sprintf("http://localhost/%s/%s/%s", org, repoName, h.String())
			loc := &remotesapi.DownloadLoc_HttpGet{HttpGet: &remotesapi.HttpGetChunk{Url: url}}
			locs = append(locs, &remotesapi.DownloadLoc{Hashes: [][]byte{tmp[:]}, Location: loc})
		}
	}

	return &remotesapi.GetDownloadLocsResponse{Locs: locs}, nil
}

func (rs RemoteChunkStore) GetUploadLocations(ctx context.Context, req *remotesapi.GetUploadLocsRequest) (*remotesapi.GetUploadLocsResponse, error) {
	logFinish := logStart("GetUploadLocations")
	defer logFinish()

	cs := getStore(req.RepoId, "GetWriteChunkUrls")

	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	org := req.RepoId.Org
	repoName := req.RepoId.RepoName
	hashes, _ := remotestorage.ParseByteSlices(req.Hashes)
	absent := cs.HasMany(hashes)

	var locs []*remotesapi.UploadLoc
	for h := range hashes {
		// if it's absent send the upload location
		if _, ok := absent[h]; ok {
			tmp := h
			url := fmt.Sprintf("http://localhost/%s/%s/%s", org, repoName, h.String())
			loc := &remotesapi.UploadLoc_HttpPost{HttpPost: &remotesapi.HttpPostChunk{Url: url}}
			locs = append(locs, &remotesapi.UploadLoc{Hashes: [][]byte{tmp[:]}, Location: loc})
		}
	}

	return &remotesapi.GetUploadLocsResponse{Locs: locs}, nil
}

func (rs RemoteChunkStore) Rebase(ctx context.Context, req *remotesapi.RebaseRequest) (*remotesapi.RebaseResponse, error) {
	logFinish := logStart("Rebase")
	defer logFinish()

	cs := getStore(req.RepoId, "Rebase")

	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	err := pantoerr.PanicToError("Rebase failed", func() error {
		cs.Rebase()
		return nil
	})

	if err != nil {
		cause := pantoerr.GetRecoveredPanicCause(err)
		log.Println("panic occurred during processing of Rebase of", req.RepoId.Org+"/"+req.RepoId.RepoName, "details", cause)
		return nil, status.Error(codes.Internal, "Failed to rebase")
	}

	return &remotesapi.RebaseResponse{}, nil
}

func (rs RemoteChunkStore) Root(ctx context.Context, req *remotesapi.RootRequest) (*remotesapi.RootResponse, error) {
	logFinish := logStart("Root")
	defer logFinish()

	cs := getStore(req.RepoId, "Root")

	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	var h hash.Hash
	err := pantoerr.PanicToError("Root failed", func() error {
		h = cs.Root()
		return nil
	})

	if err != nil {
		cause := pantoerr.GetRecoveredPanicCause(err)
		log.Println("panic occurred during processing of Root rpc of", req.RepoId.Org+"/"+req.RepoId.RepoName, "details", cause)
		return nil, status.Error(codes.Internal, "Failed to get root")
	}

	return &remotesapi.RootResponse{RootHash: h[:]}, nil
}

func (rs RemoteChunkStore) Commit(ctx context.Context, req *remotesapi.CommitRequest) (*remotesapi.CommitResponse, error) {
	logFinish := logStart("Commit")
	defer logFinish()

	cs := getStore(req.RepoId, "Commit")

	if cs == nil {
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}

	currHash := hash.New(req.Current)
	lastHash := hash.New(req.Last)

	var ok bool
	err := pantoerr.PanicToError("Commit failed", func() error {
		ok = cs.Commit(currHash, lastHash)
		return nil
	})

	if err != nil {
		cause := pantoerr.GetRecoveredPanicCause(err)
		log.Println("panic occurred during processing of Commit of", req.RepoId.Org+"/"+req.RepoId.RepoName, "last:", lastHash.String(), "curr:", currHash.String(), "details", cause)
		return nil, status.Error(codes.Internal, "Failed to rebase")
	}

	return &remotesapi.CommitResponse{Success: ok}, nil
}

func getStore(repoId *remotesapi.RepoId, rpcName string) chunks.ChunkStore {
	org := repoId.Org
	repoName := repoId.RepoName

	cs, err := csCache.Get(org, repoName)

	if err != nil {
		log.Printf("Failed to retrieve chunkstore for %s/%s\n", org, repoName)
	}

	return cs
}

func logStart(callName string) func() {
	callId := callName + ":" + uuid.New().String()
	log.Println("starting:", callId)

	return func() {
		log.Println("finished:", callId)
	}
}
