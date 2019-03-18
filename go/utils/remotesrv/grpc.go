package main

import (
	"context"
	"fmt"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/nbs"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/liquidata-inc/ld/dolt/go/gen/proto/dolt/services/remotesapi_v1alpha1"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/remotestorage"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"strings"
	"sync/atomic"
	"time"
)

type StorageLocation string

const (
	InvalidStorageLoc = "invalid"
	HttpFSStorage     = "http-file-server"
	S3Storage         = "s3"
)

func StoragLocFromString(str string) StorageLocation {
	switch strings.ToLower(str) {
	case HttpFSStorage:
		return HttpFSStorage
	case S3Storage:
		return S3Storage
	}

	return InvalidStorageLoc
}

type RemoteChunkStore struct {
	HttpHost        string
	storageLocation StorageLocation
	csCache         *DBCache
	bucket          string
	s3Api           *s3.S3
}

func NewAwsBackedChunkStore(csCache *DBCache) *RemoteChunkStore {
	return &RemoteChunkStore{
		"",
		S3Storage,
		csCache,
		csCache.bucket,
		csCache.s3Api,
	}
}

func NewHttpFSBackedChunkStore(httpHost string, csCache *DBCache) *RemoteChunkStore {
	return &RemoteChunkStore{
		httpHost,
		HttpFSStorage,
		csCache,
		"",
		nil,
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

	absent := cs.HasMany(hashes)
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
	switch rs.storageLocation {
	case HttpFSStorage:
		return fmt.Sprintf("http://%s/%s/%s/%s", rs.HttpHost, org, repoName, fileId), nil

	case S3Storage:
		req, _ := rs.s3Api.GetObjectRequest(&s3.GetObjectInput{
			Bucket: aws.String(rs.bucket),
			Key:    aws.String(fileId),
		})

		url, err := req.Presign(15 * time.Minute)

		if err != nil {
			logger(fmt.Sprintln("error presigning request", err))
			return "", err
		}

		return url, nil

	default:
		panic("Unsupported storage type " + rs.storageLocation)
	}
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
	absent := cs.HasMany(hashes)

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
	switch rs.storageLocation {
	case HttpFSStorage:
		return fmt.Sprintf("http://%s/%s/%s/%s", rs.HttpHost, org, repoName, fileId), nil

	case S3Storage:
		logger("generating s3 signed url")
		req, _ := rs.s3Api.PutObjectRequest(&s3.PutObjectInput{
			Bucket: aws.String(rs.bucket),
			Key:    aws.String(fileId),
			//ContentType: aws.String("application/octet-stream"),
		})

		//x := aws.LogDebugWithSigning
		//req.Config.LogLevel = &x
		url, err := req.Presign(2 * time.Minute)
		if err != nil {
			logger(fmt.Sprintln("error presigning request", err))
			return "", err
		}

		return url, nil

	default:
		panic("Unsupported storage type " + rs.storageLocation)
	}
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
		cs.Rebase()
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
		h = cs.Root()
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

	cs.UpdateManifest(updates)

	currHash := hash.New(req.Current)
	lastHash := hash.New(req.Last)

	var ok bool
	err := pantoerr.PanicToError("Commit failed", func() error {
		ok = cs.Commit(currHash, lastHash)
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
