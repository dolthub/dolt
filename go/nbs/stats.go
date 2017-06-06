// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"fmt"

	"github.com/attic-labs/noms/go/metrics"
)

type Stats struct {
	OpenLatency   metrics.Histogram
	CommitLatency metrics.Histogram

	GetLatency   metrics.Histogram
	ChunksPerGet metrics.Histogram

	FileReadLatency  metrics.Histogram
	FileBytesPerRead metrics.Histogram
	S3ReadLatency    metrics.Histogram
	S3BytesPerRead   metrics.Histogram

	HasLatency      metrics.Histogram
	AddressesPerHas metrics.Histogram

	PutLatency metrics.Histogram

	PersistLatency   metrics.Histogram
	BytesPerPersist  metrics.Histogram
	ChunksPerPersist metrics.Histogram

	ConjoinLatency   metrics.Histogram
	BytesPerConjoin  metrics.Histogram
	ChunksPerConjoin metrics.Histogram
	TablesPerConjoin metrics.Histogram

	ReadManifestLatency  metrics.Histogram
	WriteManifestLatency metrics.Histogram
}

func NewStats() *Stats {
	return &Stats{
		OpenLatency:          metrics.NewTimeHistogram(),
		CommitLatency:        metrics.NewTimeHistogram(),
		GetLatency:           metrics.NewTimeHistogram(),
		FileReadLatency:      metrics.NewTimeHistogram(),
		FileBytesPerRead:     metrics.NewByteHistogram(),
		S3ReadLatency:        metrics.NewTimeHistogram(),
		S3BytesPerRead:       metrics.NewByteHistogram(),
		HasLatency:           metrics.NewTimeHistogram(),
		PutLatency:           metrics.NewTimeHistogram(),
		PersistLatency:       metrics.NewTimeHistogram(),
		BytesPerPersist:      metrics.NewByteHistogram(),
		ConjoinLatency:       metrics.NewTimeHistogram(),
		BytesPerConjoin:      metrics.NewByteHistogram(),
		ReadManifestLatency:  metrics.NewTimeHistogram(),
		WriteManifestLatency: metrics.NewTimeHistogram(),
	}
}

func (s *Stats) Add(other Stats) {
	s.OpenLatency.Add(other.OpenLatency)
	s.CommitLatency.Add(other.CommitLatency)

	s.GetLatency.Add(other.GetLatency)
	s.ChunksPerGet.Add(other.ChunksPerGet)

	s.FileReadLatency.Add(other.FileReadLatency)
	s.FileBytesPerRead.Add(other.FileBytesPerRead)

	s.S3ReadLatency.Add(other.S3ReadLatency)
	s.S3BytesPerRead.Add(other.S3BytesPerRead)

	s.HasLatency.Add(other.HasLatency)
	s.AddressesPerHas.Add(other.AddressesPerHas)

	s.PutLatency.Add(other.PutLatency)

	s.PersistLatency.Add(other.PersistLatency)
	s.BytesPerPersist.Add(other.BytesPerPersist)
	s.ChunksPerPersist.Add(other.ChunksPerPersist)

	s.ConjoinLatency.Add(other.ConjoinLatency)
	s.BytesPerConjoin.Add(other.BytesPerConjoin)
	s.ChunksPerConjoin.Add(other.ChunksPerConjoin)
	s.TablesPerConjoin.Add(other.TablesPerConjoin)
}

func (s Stats) Delta(other Stats) Stats {
	return Stats{
		s.OpenLatency.Delta(other.OpenLatency),
		s.CommitLatency.Delta(other.CommitLatency),

		s.GetLatency.Delta(other.GetLatency),
		s.ChunksPerGet.Delta(other.ChunksPerGet),

		s.FileReadLatency.Delta(other.FileReadLatency),
		s.FileBytesPerRead.Delta(other.FileBytesPerRead),

		s.S3ReadLatency.Delta(other.S3ReadLatency),
		s.S3BytesPerRead.Delta(other.S3BytesPerRead),

		s.HasLatency.Delta(other.HasLatency),
		s.AddressesPerHas.Delta(other.AddressesPerHas),

		s.PutLatency.Delta(other.PutLatency),

		s.PersistLatency.Delta(other.PersistLatency),
		s.BytesPerPersist.Delta(other.BytesPerPersist),
		s.ChunksPerPersist.Delta(other.ChunksPerPersist),

		s.ConjoinLatency.Delta(other.ConjoinLatency),
		s.BytesPerConjoin.Delta(other.BytesPerConjoin),
		s.ChunksPerConjoin.Delta(other.ChunksPerConjoin),
		s.TablesPerConjoin.Delta(other.TablesPerConjoin),

		s.ReadManifestLatency.Delta(other.ReadManifestLatency),
		s.WriteManifestLatency.Delta(other.WriteManifestLatency),
	}
}

func (s Stats) String() string {
	return fmt.Sprintf(`---NBS Stats---
OpenLatecy:           %s
CommitLatency:        %s
GetLatency:           %s
ChunksPerGet:         %s
FileReadLatency:      %s
FileBytesPerRead:     %s
S3ReadLatency:        %s
S3BytesPerRead:       %s
HasLatency:           %s
AddressesHasGet:      %s
PutLatency:           %s
PersistLatency:       %s
ChunksPerPersist:     %s
BytesPerPersist:      %s
ConjoinLatency:       %s
ChunksPerConjoin:     %s
BytesPerConjoin:      %s
TablesPerConjoin:     %s
ReadManifestLatency:  %s
WriteManifestLatency: %s
`,
		s.OpenLatency,
		s.CommitLatency,

		s.GetLatency,
		s.ChunksPerGet,

		s.FileReadLatency,
		s.FileBytesPerRead,

		s.S3ReadLatency,
		s.S3BytesPerRead,

		s.HasLatency,
		s.AddressesPerHas,

		s.PutLatency,

		s.PersistLatency,
		s.ChunksPerPersist,
		s.BytesPerPersist,

		s.ConjoinLatency,
		s.ChunksPerConjoin,
		s.BytesPerConjoin,
		s.TablesPerConjoin,
		s.ReadManifestLatency,
		s.WriteManifestLatency)
}
