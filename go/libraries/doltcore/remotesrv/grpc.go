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

package remotesrv

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrUnimplemented = errors.New("unimplemented")

const RepoPathField = "repo_path"

type RemoteChunkStore struct {
	HttpHost   string
	httpScheme string

	concurrencyControl remotesapi.PushConcurrencyControl

	csCache DBCache
	bucket  string
	fs      filesys.Filesys
	lgr     *logrus.Entry
	sealer  Sealer

	// Feature flags this server implements but will not advertise in
	// GetRepoMetadataResponse.features. The RPCs themselves stay
	// enabled — this only suppresses capability advertisement, so
	// older-client fallback paths can be exercised in tests against
	// a fully-capable server. See the plan's step 7 for rationale.
	disabledFeatures []remotesapi.Feature

	remotesapi.UnimplementedChunkStoreServiceServer
}

func NewHttpFSBackedChunkStore(lgr *logrus.Entry, httpHost string, csCache DBCache, fs filesys.Filesys, scheme string, concurrencyControl remotesapi.PushConcurrencyControl, sealer Sealer, disabledFeatures []remotesapi.Feature) *RemoteChunkStore {
	if concurrencyControl == remotesapi.PushConcurrencyControl_PUSH_CONCURRENCY_CONTROL_UNSPECIFIED {
		concurrencyControl = remotesapi.PushConcurrencyControl_PUSH_CONCURRENCY_CONTROL_IGNORE_WORKING_SET
	}
	return &RemoteChunkStore{
		HttpHost:           httpHost,
		httpScheme:         scheme,
		concurrencyControl: concurrencyControl,
		csCache:            csCache,
		bucket:             "",
		fs:                 fs,
		lgr: lgr.WithFields(logrus.Fields{
			"service": "dolt.services.remotesapi.v1alpha1.ChunkStoreServiceServer",
		}),
		sealer:           sealer,
		disabledFeatures: disabledFeatures,
	}
}

type repoRequest interface {
	GetRepoId() *remotesapi.RepoId
	GetRepoPath() string
}

func getRepoPath(req repoRequest) string {
	if req.GetRepoPath() != "" {
		return req.GetRepoPath()
	}
	if repoId := req.GetRepoId(); repoId != nil {
		return repoId.Org + "/" + repoId.RepoName
	}
	panic("unexpected empty repo_path and nil repo_id")
}

func (rs *RemoteChunkStore) HasChunks(ctx context.Context, req *remotesapi.HasChunksRequest) (*remotesapi.HasChunksResponse, error) {
	logger := getReqLogger(rs.lgr, "HasChunks")
	if err := ValidateHasChunksRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	repoPath := getRepoPath(req)
	logger = logger.WithField(RepoPathField, repoPath)
	defer func() { logger.Trace("finished") }()

	cs, err := rs.getStore(ctx, logger, repoPath)
	if err != nil {
		return nil, err
	}

	hashes, hashToIndex := remotestorage.ParseByteSlices(req.Hashes)

	absent, err := cs.HasMany(ctx, hashes)
	if err != nil {
		logger.WithError(err).Error("error calling HasMany")
		return nil, status.Error(codes.Internal, "HasMany failure:"+err.Error())
	}

	indices := make([]int32, len(absent))

	n := 0
	for h := range absent {
		indices[n] = int32(hashToIndex[h])
		n++
	}

	resp := &remotesapi.HasChunksResponse{
		Absent: indices,
	}

	logger = logger.WithFields(logrus.Fields{
		"num_requested": len(hashToIndex),
		"num_absent":    len(indices),
	})

	return resp, nil
}

func (rs *RemoteChunkStore) getRelativeStorePath(cs RemoteSrvStore) (string, error) {
	cspath, ok := cs.Path()
	if !ok {
		return "", status.Error(codes.Internal, "chunkstore misconfigured; cannot generate HTTP paths")
	}
	httproot, err := rs.fs.Abs(".")
	if err != nil {
		return "", err
	}
	prefix, err := filepath.Rel(httproot, cspath)
	if err != nil {
		return "", err
	}
	return prefix, nil
}

func (rs *RemoteChunkStore) GetDownloadLocations(ctx context.Context, req *remotesapi.GetDownloadLocsRequest) (*remotesapi.GetDownloadLocsResponse, error) {
	logger := getReqLogger(rs.lgr, "GetDownloadLocations")
	if err := ValidateGetDownloadLocsRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	repoPath := getRepoPath(req)
	logger = logger.WithField(RepoPathField, repoPath)
	defer func() { logger.Trace("finished") }()

	cs, err := rs.getStore(ctx, logger, repoPath)
	if err != nil {
		return nil, err
	}

	hashes, _ := remotestorage.ParseByteSlices(req.ChunkHashes)

	prefix, err := rs.getRelativeStorePath(cs)
	if err != nil {
		logger.WithError(err).Error("error getting file store path for chunk store")
		return nil, err
	}

	numHashes := len(hashes)

	locations, err := cs.GetChunkLocationsWithPaths(ctx, hashes)
	if err != nil {
		logger.WithError(err).Error("error getting chunk locations for hashes")
		return nil, err
	}

	md, _ := metadata.FromIncomingContext(ctx)

	var locs []*remotesapi.DownloadLoc
	numRanges := 0
	for loc, hashToRange := range locations {
		if len(hashToRange) == 0 {
			continue
		}

		numRanges += len(hashToRange)

		var ranges []*remotesapi.RangeChunk
		for h, r := range hashToRange {
			if r.DictLength != 0 {
				return nil, status.Error(codes.Unknown, "upgrade your dolt client; it is too old to read these files")
			}

			hCpy := h
			ranges = append(ranges, &remotesapi.RangeChunk{Hash: hCpy[:], Offset: r.Offset, Length: r.Length})
		}

		url := rs.getDownloadUrl(md, prefix+"/"+loc)
		preurl := url.String()
		url, err = rs.sealer.Seal(url)
		if err != nil {
			logger.WithError(err).Error("error sealing download url")
			return nil, err
		}
		logger.WithFields(logrus.Fields{
			"url":        preurl,
			"ranges":     ranges,
			"sealed_url": url.String(),
		}).Trace("generated sealed url")

		getRange := &remotesapi.HttpGetRange{Url: url.String(), Ranges: ranges}
		locs = append(locs, &remotesapi.DownloadLoc{Location: &remotesapi.DownloadLoc_HttpGetRange{HttpGetRange: getRange}})
	}

	logger = logger.WithFields(logrus.Fields{
		"num_requested": numHashes,
		"num_urls":      len(locations),
		"num_ranges":    numRanges,
	})

	return &remotesapi.GetDownloadLocsResponse{Locs: locs}, nil
}

func (rs *RemoteChunkStore) StreamDownloadLocations(stream remotesapi.ChunkStoreService_StreamDownloadLocationsServer) error {
	ologger := getReqLogger(rs.lgr, "StreamDownloadLocations")
	numMessages := 0
	numHashes := 0
	numUrls := 0
	numRanges := 0
	defer func() {
		ologger.WithFields(logrus.Fields{
			"num_messages":  numMessages,
			"num_requested": numHashes,
			"num_urls":      numUrls,
			"num_ranges":    numRanges,
		}).Trace("finished")
	}()
	logger := ologger

	md, _ := metadata.FromIncomingContext(stream.Context())

	var repoPath string
	var cs RemoteSrvStore
	var prefix string
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		numMessages += 1

		if err := ValidateGetDownloadLocsRequest(req); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}

		nextPath := getRepoPath(req)
		if nextPath != repoPath {
			repoPath = nextPath
			logger = ologger.WithField(RepoPathField, repoPath)
			cs, err = rs.getStore(stream.Context(), logger, repoPath)
			if err != nil {
				return err
			}
			prefix, err = rs.getRelativeStorePath(cs)
			if err != nil {
				logger.WithError(err).Error("error getting file store path for chunk store")
				return err
			}
		}

		hashes, _ := remotestorage.ParseByteSlices(req.ChunkHashes)
		if err != nil {
			return err
		}
		numHashes += len(hashes)
		locations, err := cs.GetChunkLocationsWithPaths(stream.Context(), hashes)
		if err != nil {
			logger.WithError(err).Error("error getting chunk locations for hashes")
			return err
		}

		var locs []*remotesapi.DownloadLoc
		for loc, hashToRange := range locations {
			if len(hashToRange) == 0 {
				continue
			}

			numUrls += 1
			numRanges += len(hashToRange)

			var ranges []*remotesapi.RangeChunk
			for h, r := range hashToRange {
				hCpy := h
				ranges = append(ranges, &remotesapi.RangeChunk{
					Hash:             hCpy[:],
					Offset:           r.Offset,
					Length:           r.Length,
					DictionaryOffset: r.DictOffset,
					DictionaryLength: r.DictLength})
			}

			url := rs.getDownloadUrl(md, prefix+"/"+loc)
			preurl := url.String()
			url, err = rs.sealer.Seal(url)
			if err != nil {
				logger.WithError(err).Error("error sealing download url")
				return err
			}
			logger.WithFields(logrus.Fields{
				"url":        preurl,
				"ranges":     ranges,
				"sealed_url": url.String(),
			}).Trace("generated sealed url")

			getRange := &remotesapi.HttpGetRange{Url: url.String(), Ranges: ranges}
			locs = append(locs, &remotesapi.DownloadLoc{Location: &remotesapi.DownloadLoc_HttpGetRange{HttpGetRange: getRange}})
		}

		if err := stream.Send(&remotesapi.GetDownloadLocsResponse{Locs: locs}); err != nil {
			return err
		}
	}
}

func (rs *RemoteChunkStore) StreamChunkLocations(stream remotesapi.ChunkStoreService_StreamChunkLocationsServer) error {
	ologger := getReqLogger(rs.lgr, "StreamChunkLocations")
	numMessages := 0
	numHashes := 0
	numNewTableFiles := 0
	numLocations := 0
	numMissing := 0
	defer func() {
		ologger.WithFields(logrus.Fields{
			"num_messages":        numMessages,
			"num_requested":       numHashes,
			"num_new_table_files": numNewTableFiles,
			"num_locations":       numLocations,
			"num_missing":         numMissing,
		}).Trace("finished")
	}()
	logger := ologger

	md, _ := metadata.FromIncomingContext(stream.Context())

	var repoPath string
	var cs RemoteSrvStore
	var prefix string

	// Stream-local table-file-path -> table_file_id map. Scoped to this
	// handler invocation. Discarded on handler exit; a fresh handler
	// after a client-side reliable reconnect starts from an empty map
	// and re-introduces any id it reuses. The client relies on
	// TableFileRecord overwrite semantics to make that transparent.
	tfByPath := make(map[string]uint32)
	var nextTFID uint32

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		numMessages++

		if err := ValidateStreamChunkLocationsRequest(req); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}

		nextPath := getRepoPath(req)
		if nextPath != repoPath {
			repoPath = nextPath
			logger = ologger.WithField(RepoPathField, repoPath)
			cs, err = rs.getStore(stream.Context(), logger, repoPath)
			if err != nil {
				return err
			}
			prefix, err = rs.getRelativeStorePath(cs)
			if err != nil {
				logger.WithError(err).Error("error getting file store path for chunk store")
				return err
			}
		}

		// req.ChunkHashes is a flat 20-byte-per-hash buffer (validated
		// above). Walk it to build a HashSet to pass to
		// GetChunkLocationsWithPaths and the position-in-request
		// index lookup used for request_index / missing_indexes.
		// hash.New copies each 20-byte sub-slice into a Hash value,
		// so no heap allocation per element.
		n := len(req.ChunkHashes) / hash.ByteLen
		hashes := make(hash.HashSet, n)
		hashToIndex := make(map[hash.Hash]int, n)
		for i := 0; i < n; i++ {
			h := hash.New(req.ChunkHashes[i*hash.ByteLen : (i+1)*hash.ByteLen])
			hashes[h] = struct{}{}
			hashToIndex[h] = i
		}
		numHashes += n

		// GetChunkLocationsWithPaths deletes found hashes from
		// |hashes|; the remainder is exactly the set the server
		// could not find.
		locations, err := cs.GetChunkLocationsWithPaths(stream.Context(), hashes)
		if err != nil {
			logger.WithError(err).Error("error getting chunk locations for hashes")
			return err
		}

		var tableFiles []*remotesapi.StreamChunkLocationsResponse_TableFileRecord
		var chunkLocs []*remotesapi.StreamChunkLocationsResponse_ChunkLocation

		for path, hashToRange := range locations {
			if len(hashToRange) == 0 {
				continue
			}

			id, seen := tfByPath[path]
			if !seen {
				id = nextTFID
				nextTFID++
				tfByPath[path] = id

				u := rs.getDownloadUrl(md, prefix+"/"+path)
				preurl := u.String()
				u, err = rs.sealer.Seal(u)
				if err != nil {
					logger.WithError(err).Error("error sealing download url")
					return err
				}
				logger.WithFields(logrus.Fields{
					"url":           preurl,
					"sealed_url":    u.String(),
					"table_file_id": id,
				}).Trace("introducing table file record")

				tableFiles = append(tableFiles, &remotesapi.StreamChunkLocationsResponse_TableFileRecord{
					TableFileId: id,
					Url:         u.String(),
					FileId:      path,
				})
				numNewTableFiles++
			}

			for h, r := range hashToRange {
				chunkLocs = append(chunkLocs, &remotesapi.StreamChunkLocationsResponse_ChunkLocation{
					TableFileId:      id,
					RequestIndex:     uint32(hashToIndex[h]),
					Offset:           r.Offset,
					Length:           r.Length,
					DictionaryOffset: r.DictOffset,
					DictionaryLength: r.DictLength,
				})
				numLocations++
			}
		}

		var missing []uint32
		if len(hashes) > 0 {
			missing = make([]uint32, 0, len(hashes))
			for h := range hashes {
				missing = append(missing, uint32(hashToIndex[h]))
			}
			numMissing += len(missing)
		}

		if err := stream.Send(&remotesapi.StreamChunkLocationsResponse{
			TableFiles:     tableFiles,
			Locations:      chunkLocs,
			MissingIndexes: missing,
		}); err != nil {
			return err
		}
	}
}

func (rs *RemoteChunkStore) getHost(md metadata.MD) string {
	host := rs.HttpHost
	if strings.HasPrefix(rs.HttpHost, ":") {
		hosts := md.Get(":authority")
		if len(hosts) > 0 {
			host = strings.Split(hosts[0], ":")[0] + rs.HttpHost
		}
	} else if rs.HttpHost == "" {
		hosts := md.Get(":authority")
		if len(hosts) > 0 {
			host = hosts[0]
		}
	}
	return host
}

func (rs *RemoteChunkStore) getScheme(md metadata.MD) string {
	scheme := rs.httpScheme
	forwardedSchemes := md.Get("x-forwarded-proto")
	if len(forwardedSchemes) > 0 {
		scheme = forwardedSchemes[0]
	}
	return scheme
}

func (rs *RemoteChunkStore) getDownloadUrl(md metadata.MD, path string) *url.URL {
	host := rs.getHost(md)
	scheme := rs.getScheme(md)
	return &url.URL{
		Scheme: scheme,
		Host:   host,
		Path:   path,
	}
}

func getTableFileDetails(req *remotesapi.GetUploadLocsRequest) ([]*remotesapi.TableFileDetails, error) {
	tfd := req.GetTableFileDetails()
	if len(tfd) == 0 {
		return nil, errors.New("no table file details provided. Your dolt version is pre 1.0. please upgrade.")
	}
	return tfd, nil
}

func (rs *RemoteChunkStore) GetUploadLocations(ctx context.Context, req *remotesapi.GetUploadLocsRequest) (*remotesapi.GetUploadLocsResponse, error) {
	logger := getReqLogger(rs.lgr, "GetUploadLocations")
	if err := ValidateGetUploadLocsRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	repoPath := getRepoPath(req)
	logger = logger.WithField(RepoPathField, repoPath)
	defer func() { logger.Trace("finished") }()

	_, err := rs.getStore(ctx, logger, repoPath)
	if err != nil {
		return nil, err
	}

	tfds, err := getTableFileDetails(req)
	if err != nil {
		return nil, err
	}

	md, _ := metadata.FromIncomingContext(ctx)

	var locs []*remotesapi.UploadLoc
	for _, tfd := range tfds {
		h := hash.New(tfd.Id)
		url := rs.getUploadUrl(md, repoPath, tfd)
		url, err = rs.sealer.Seal(url)
		if err != nil {
			logger.WithError(err).Error("error sealing upload url")
			return nil, status.Error(codes.Internal, "Failed to seal upload Url.")
		}

		loc := &remotesapi.UploadLoc_HttpPost{HttpPost: &remotesapi.HttpPostTableFile{Url: url.String()}}
		locs = append(locs, &remotesapi.UploadLoc{TableFileHash: h[:], Location: loc})

		logger.WithFields(logrus.Fields{
			"table_file_hash": h.String(),
			"url":             url.String(),
		}).Trace("sending upload location for table file")
	}

	logger = logger.WithFields(logrus.Fields{
		"num_urls": len(locs),
	})

	return &remotesapi.GetUploadLocsResponse{Locs: locs}, nil
}

func (rs *RemoteChunkStore) getUploadUrl(md metadata.MD, repoPath string, tfd *remotesapi.TableFileDetails) *url.URL {
	fileID := hash.New(tfd.Id).String() + tfd.Suffix
	params := url.Values{}
	params.Add("num_chunks", strconv.Itoa(int(tfd.NumChunks)))
	params.Add("split_offset", strconv.Itoa(int(tfd.SplitOffset)))
	params.Add("content_length", strconv.Itoa(int(tfd.ContentLength)))
	params.Add("content_hash", base64.RawURLEncoding.EncodeToString(tfd.ContentHash))
	scheme := rs.getScheme(md)
	return &url.URL{
		Scheme:   scheme,
		Host:     rs.getHost(md),
		Path:     fmt.Sprintf("%s/%s", repoPath, fileID),
		RawQuery: params.Encode(),
	}
}

func (rs *RemoteChunkStore) Rebase(ctx context.Context, req *remotesapi.RebaseRequest) (*remotesapi.RebaseResponse, error) {
	logger := getReqLogger(rs.lgr, "Rebase")
	if err := ValidateRebaseRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	repoPath := getRepoPath(req)
	logger = logger.WithField(RepoPathField, repoPath)
	defer func() { logger.Trace("finished") }()

	_, err := rs.getStore(ctx, logger, repoPath)
	if err != nil {
		return nil, err
	}

	return &remotesapi.RebaseResponse{}, nil
}

func (rs *RemoteChunkStore) Root(ctx context.Context, req *remotesapi.RootRequest) (*remotesapi.RootResponse, error) {
	logger := getReqLogger(rs.lgr, "Root")
	if err := ValidateRootRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	repoPath := getRepoPath(req)
	logger = logger.WithField(RepoPathField, repoPath)
	defer func() { logger.Trace("finished") }()

	cs, err := rs.getStore(ctx, logger, repoPath)
	if err != nil {
		return nil, err
	}

	h, err := cs.Root(ctx)
	if err != nil {
		logger.WithError(err).Error("error calling Root on chunk store.")
		return nil, status.Error(codes.Internal, "Failed to get root")
	}

	return &remotesapi.RootResponse{RootHash: h[:]}, nil
}

func (rs *RemoteChunkStore) Commit(ctx context.Context, req *remotesapi.CommitRequest) (*remotesapi.CommitResponse, error) {
	logger := getReqLogger(rs.lgr, "Commit")
	if err := ValidateCommitRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	repoPath := getRepoPath(req)
	logger = logger.WithField(RepoPathField, repoPath)
	defer func() { logger.Trace("finished") }()

	cs, err := rs.getStore(ctx, logger, repoPath)
	if err != nil {
		return nil, err
	}

	updates := make(map[string]int)
	for _, cti := range req.ChunkTableInfo {
		updates[hash.New(cti.Hash).String()] = int(cti.ChunkCount)
	}

	err = cs.AddTableFilesToManifest(ctx, updates, rs.getAddrs(cs.Version()))
	if err != nil {
		logger.WithError(err).Error("error calling AddTableFilesToManifest")
		code := codes.Internal
		if errors.Is(err, nbs.ErrDanglingRef) || errors.Is(err, nbs.ErrTableFileNotFound) {
			code = codes.FailedPrecondition
		}
		return nil, status.Errorf(code, "manifest update error: %v", err)
	}

	currHash := hash.New(req.Current)
	lastHash := hash.New(req.Last)

	var ok bool
	ok, err = cs.Commit(ctx, currHash, lastHash)
	if err != nil {
		logger.WithError(err).WithFields(logrus.Fields{
			"last_hash": lastHash.String(),
			"curr_hash": currHash.String(),
		}).Error("error calling Commit")
		code := codes.Internal
		if errors.Is(err, nbs.ErrDanglingRef) || errors.Is(err, nbs.ErrTableFileNotFound) {
			code = codes.FailedPrecondition
		}
		return nil, status.Errorf(code, "failed to commit: %v", err)
	}

	logger.Tracef("Commit success; moved from %s -> %s", lastHash.String(), currHash.String())
	return &remotesapi.CommitResponse{Success: ok}, nil
}

func (rs *RemoteChunkStore) GetRepoMetadata(ctx context.Context, req *remotesapi.GetRepoMetadataRequest) (*remotesapi.GetRepoMetadataResponse, error) {
	logger := getReqLogger(rs.lgr, "GetRepoMetadata")
	if err := ValidateGetRepoMetadataRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	repoPath := getRepoPath(req)
	logger = logger.WithField(RepoPathField, repoPath)
	defer func() { logger.Trace("finished") }()

	cs, err := rs.getOrCreateStore(ctx, logger, repoPath, req.ClientRepoFormat.NbfVersion)
	if err != nil {
		return nil, err
	}

	size, err := cs.Size(ctx)
	if err != nil {
		logger.WithError(err).Error("error calling Size")
		return nil, err
	}

	return &remotesapi.GetRepoMetadataResponse{
		NbfVersion:             cs.Version(),
		NbsVersion:             req.ClientRepoFormat.NbsVersion,
		StorageSize:            size,
		PushConcurrencyControl: rs.concurrencyControl,
		Features:               rs.advertisedFeatures(),
	}, nil
}

// supportedFeatures is the canonical list of Feature flags this build
// of remotesrv implements. Advertised set is this list minus
// rs.disabledFeatures. Append new features here when they land; do not
// hand-roll per-feature booleans.
var supportedFeatures = []remotesapi.Feature{
	remotesapi.Feature_FEATURE_STREAM_CHUNK_LOCATIONS,
}

func (rs *RemoteChunkStore) advertisedFeatures() []remotesapi.Feature {
	if len(rs.disabledFeatures) == 0 {
		return supportedFeatures
	}
	out := make([]remotesapi.Feature, 0, len(supportedFeatures))
	for _, f := range supportedFeatures {
		if !slices.Contains(rs.disabledFeatures, f) {
			out = append(out, f)
		}
	}
	return out
}

func (rs *RemoteChunkStore) ListTableFiles(ctx context.Context, req *remotesapi.ListTableFilesRequest) (*remotesapi.ListTableFilesResponse, error) {
	logger := getReqLogger(rs.lgr, "ListTableFiles")
	if err := ValidateListTableFilesRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	repoPath := getRepoPath(req)
	logger = logger.WithField(RepoPathField, repoPath)
	defer func() { logger.Trace("finished") }()

	cs, err := rs.getStore(ctx, logger, repoPath)
	if err != nil {
		return nil, err
	}

	tfsources, err := cs.Sources(ctx)
	if err != nil {
		logger.WithError(err).Error("error getting chunk store Sources")
		return nil, status.Error(codes.Internal, "failed to get sources")
	}
	root, tables, appendixTables := tfsources.Root, tfsources.TableFiles, tfsources.AppendixTableFiles

	md, _ := metadata.FromIncomingContext(ctx)

	tableFileInfo, err := getTableFileInfo(logger, md, rs, tables, req, cs)
	if err != nil {
		logger.WithError(err).Error("error getting table file info")
		return nil, err
	}

	appendixTableFileInfo, err := getTableFileInfo(logger, md, rs, appendixTables, req, cs)
	if err != nil {
		logger.WithError(err).Error("error getting appendix table file info")
		return nil, err
	}

	logger = logger.WithFields(logrus.Fields{
		"num_table_files":          len(tableFileInfo),
		"num_appendix_table_files": len(appendixTableFileInfo),
	})

	resp := &remotesapi.ListTableFilesResponse{
		RootHash:              root[:],
		TableFileInfo:         tableFileInfo,
		AppendixTableFileInfo: appendixTableFileInfo,
	}

	return resp, nil
}

func getTableFileInfo(
	logger *logrus.Entry,
	md metadata.MD,
	rs *RemoteChunkStore,
	tableList []chunks.TableFile,
	req *remotesapi.ListTableFilesRequest,
	cs RemoteSrvStore,
) ([]*remotesapi.TableFileInfo, error) {
	prefix, err := rs.getRelativeStorePath(cs)
	if err != nil {
		return nil, err
	}
	appendixTableFileInfo := make([]*remotesapi.TableFileInfo, 0)
	for _, t := range tableList {
		url := rs.getDownloadUrl(md, prefix+"/"+t.LocationPrefix()+t.FileID()+t.LocationSuffix())
		url, err = rs.sealer.Seal(url)
		if err != nil {
			return nil, status.Error(codes.Internal, "failed to get seal download url for "+t.FileID())
		}

		appendixTableFileInfo = append(appendixTableFileInfo, &remotesapi.TableFileInfo{
			FileId:    t.FileID(),
			NumChunks: uint32(t.NumChunks()),
			Url:       url.String(),
		})
	}
	return appendixTableFileInfo, nil
}

// AddTableFiles updates the remote manifest with new table files without modifying the root hash.
func (rs *RemoteChunkStore) AddTableFiles(ctx context.Context, req *remotesapi.AddTableFilesRequest) (*remotesapi.AddTableFilesResponse, error) {
	logger := getReqLogger(rs.lgr, "AddTableFiles")
	if err := ValidateAddTableFilesRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	repoPath := getRepoPath(req)
	logger = logger.WithField(RepoPathField, repoPath)
	defer func() { logger.Trace("finished") }()

	cs, err := rs.getStore(ctx, logger, repoPath)
	if err != nil {
		return nil, err
	}

	updates := make(map[string]int)
	for _, cti := range req.ChunkTableInfo {
		updates[hash.New(cti.Hash).String()] = int(cti.ChunkCount)
	}

	err = cs.AddTableFilesToManifest(ctx, updates, rs.getAddrs(cs.Version()))
	if err != nil {
		logger.WithError(err).Error("error occurred updating the manifest")
		code := codes.Internal
		if errors.Is(err, nbs.ErrDanglingRef) || errors.Is(err, nbs.ErrTableFileNotFound) {
			code = codes.FailedPrecondition
		}
		return nil, status.Error(code, "manifest update error")
	}

	logger = logger.WithFields(logrus.Fields{
		"num_files": len(updates),
	})

	return &remotesapi.AddTableFilesResponse{Success: true}, nil
}

// Returns a |chunks.InsertAddrsCurry| for the nbf (NomsBinFormat)
// corresponding to |version|.
//
// Used to implement chunk reference sanity checks when adding table files that have
// been uploaded by clients to the stores managed by the gRPC server.
func (rs *RemoteChunkStore) getAddrs(version string) chunks.InsertAddrsCurry {
	fmt, err := types.GetFormatForVersionString(version)
	if err != nil {
		panic("unexpxected error on GetFormatForVersionString")
	}
	return func(c chunks.Chunk) chunks.InsertAddrsCb {
		return func(ctx context.Context, addrs hash.HashSet, _ chunks.PendingRefExists) error {
			return types.InsertAddrsFromNomsValue(c, fmt, addrs)
		}
	}
}

func (rs *RemoteChunkStore) getStore(ctx context.Context, logger *logrus.Entry, repoPath string) (RemoteSrvStore, error) {
	return rs.getOrCreateStore(ctx, logger, repoPath, types.Format_Default.VersionString())
}

func (rs *RemoteChunkStore) getOrCreateStore(ctx context.Context, logger *logrus.Entry, repoPath, nbfVerStr string) (RemoteSrvStore, error) {
	cs, err := rs.csCache.Get(ctx, repoPath, nbfVerStr)
	if err != nil {
		logger.WithError(err).Error("Failed to retrieve chunkstore")
		if errors.Is(err, ErrUnimplemented) {
			return nil, status.Error(codes.Unimplemented, err.Error())
		}
		return nil, err
	}
	if cs == nil {
		logger.Error("internal error getting chunk store; csCache.Get returned nil")
		return nil, status.Error(codes.Internal, "Could not get chunkstore")
	}
	return cs, nil
}

var requestId int32

func incReqId() int {
	return int(atomic.AddInt32(&requestId, 1))
}

func getReqLogger(lgr *logrus.Entry, method string) *logrus.Entry {
	lgr = lgr.WithFields(logrus.Fields{
		"method":      method,
		"request_num": strconv.Itoa(incReqId()),
	})
	lgr.Trace("starting request")
	return lgr
}

type ReadOnlyChunkStore struct {
	remotesapi.ChunkStoreServiceServer
}

func (rs ReadOnlyChunkStore) GetUploadLocations(ctx context.Context, req *remotesapi.GetUploadLocsRequest) (*remotesapi.GetUploadLocsResponse, error) {
	return nil, status.Error(codes.PermissionDenied, "this server only provides read-only access")
}

func (rs ReadOnlyChunkStore) AddTableFiles(ctx context.Context, req *remotesapi.AddTableFilesRequest) (*remotesapi.AddTableFilesResponse, error) {
	return nil, status.Error(codes.PermissionDenied, "this server only provides read-only access")
}

func (rs ReadOnlyChunkStore) Commit(ctx context.Context, req *remotesapi.CommitRequest) (*remotesapi.CommitResponse, error) {
	return nil, status.Error(codes.PermissionDenied, "this server only provides read-only access")
}
