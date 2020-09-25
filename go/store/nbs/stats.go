// Copyright 2019 Liquidata, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"fmt"

	"github.com/dolthub/dolt/go/store/metrics"
)

type Stats struct {
	OpenLatency   metrics.Histogram
	CommitLatency metrics.Histogram

	IndexReadLatency  metrics.Histogram
	IndexBytesPerRead metrics.Histogram

	GetLatency   metrics.Histogram
	ChunksPerGet metrics.Histogram

	FileReadLatency  metrics.Histogram
	FileBytesPerRead metrics.Histogram

	S3ReadLatency  metrics.Histogram
	S3BytesPerRead metrics.Histogram

	MemReadLatency  metrics.Histogram
	MemBytesPerRead metrics.Histogram

	DynamoReadLatency  metrics.Histogram
	DynamoBytesPerRead metrics.Histogram

	HasLatency      metrics.Histogram
	AddressesPerHas metrics.Histogram

	PutLatency metrics.Histogram

	PersistLatency  metrics.Histogram
	BytesPerPersist metrics.Histogram

	ChunksPerPersist                 metrics.Histogram
	CompressedChunkBytesPerPersist   metrics.Histogram
	UncompressedChunkBytesPerPersist metrics.Histogram

	ConjoinLatency   metrics.Histogram
	BytesPerConjoin  metrics.Histogram
	ChunksPerConjoin metrics.Histogram
	TablesPerConjoin metrics.Histogram

	ReadManifestLatency  metrics.Histogram
	WriteManifestLatency metrics.Histogram
}

func NewStats() *Stats {
	return &Stats{
		OpenLatency:                      metrics.NewTimeHistogram(),
		CommitLatency:                    metrics.NewTimeHistogram(),
		IndexReadLatency:                 metrics.NewTimeHistogram(),
		IndexBytesPerRead:                metrics.NewByteHistogram(),
		GetLatency:                       metrics.NewTimeHistogram(),
		FileReadLatency:                  metrics.NewTimeHistogram(),
		FileBytesPerRead:                 metrics.NewByteHistogram(),
		S3ReadLatency:                    metrics.NewTimeHistogram(),
		S3BytesPerRead:                   metrics.NewByteHistogram(),
		MemReadLatency:                   metrics.NewTimeHistogram(),
		MemBytesPerRead:                  metrics.NewByteHistogram(),
		DynamoReadLatency:                metrics.NewTimeHistogram(),
		DynamoBytesPerRead:               metrics.NewByteHistogram(),
		HasLatency:                       metrics.NewTimeHistogram(),
		PutLatency:                       metrics.NewTimeHistogram(),
		PersistLatency:                   metrics.NewTimeHistogram(),
		BytesPerPersist:                  metrics.NewByteHistogram(),
		CompressedChunkBytesPerPersist:   metrics.NewByteHistogram(),
		UncompressedChunkBytesPerPersist: metrics.NewByteHistogram(),
		ConjoinLatency:                   metrics.NewTimeHistogram(),
		BytesPerConjoin:                  metrics.NewByteHistogram(),
		ReadManifestLatency:              metrics.NewTimeHistogram(),
		WriteManifestLatency:             metrics.NewTimeHistogram(),
	}
}

func (s Stats) String() string {
	return fmt.Sprintf(`---NBS Stats---
OpenLatecy:                       %s
CommitLatency:                    %s
IndexReadLatency:                 %s
IndexBytesPerRead:                %s
GetLatency:                       %s
ChunksPerGet:                     %s
FileReadLatency:                  %s
FileBytesPerRead:                 %s
S3ReadLatency:                    %s
S3BytesPerRead:                   %s
MemReadLatency:                   %s
MemBytesPerRead:                  %s
DynamoReadLatency:                %s
DynamoBytesPerRead:               %s
HasLatency:                       %s
AddressesHasGet:                  %s
PutLatency:                       %s
PersistLatency:                   %s
BytesPerPersist:                  %s
ChunksPerPersist:                 %s
CompressedChunkBytesPerPersist:   %s
UncompressedChunkBytesPerPersist: %s
ConjoinLatency:                   %s
BytesPerConjoin:                  %s
ChunksPerConjoin:                 %s
TablesPerConjoin:                 %s
ReadManifestLatency:              %s
WriteManifestLatency:             %s
`,
		s.OpenLatency,
		s.CommitLatency,

		s.IndexReadLatency,
		s.IndexBytesPerRead,

		s.GetLatency,
		s.ChunksPerGet,

		s.FileReadLatency,
		s.FileBytesPerRead,

		s.S3ReadLatency,
		s.S3BytesPerRead,

		s.MemReadLatency,
		s.MemBytesPerRead,

		s.DynamoReadLatency,
		s.DynamoBytesPerRead,

		s.HasLatency,
		s.AddressesPerHas,

		s.PutLatency,

		s.PersistLatency,
		s.BytesPerPersist,

		s.ChunksPerPersist,
		s.CompressedChunkBytesPerPersist,
		s.UncompressedChunkBytesPerPersist,

		s.ConjoinLatency,
		s.BytesPerConjoin,
		s.ChunksPerConjoin,
		s.TablesPerConjoin,
		s.ReadManifestLatency,
		s.WriteManifestLatency)
}
