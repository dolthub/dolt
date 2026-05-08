// Copyright 2026 Dolthub, Inc.
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

package datas

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// RefClassifier is a function that determines whether a dataset ID represents
// an "old generation" reference. Old generation refs are stable, long-lived
// references (e.g., branches, remotes) while new generation refs are ephemeral
// (e.g., tags, workspaces). The GC uses this distinction to run a two-phase
// collection: conservative sweep of oldgen first, then aggressive sweep of
// newgen.
//
// Returns true if the dataset ID should be classified as old generation.
// Returns false for new generation or if the ref type is unknown.
type RefClassifier func(datasetID string) (isOldGen bool)

// CollectGarbage runs garbage collection on a Database. It iterates all
// datasets, classifies each into oldGen or newGen using the provided
// classifier, then invokes the underlying GC.
//
// The classifier function determines which refs are stable (oldGen) vs
// ephemeral (newGen). Callers that use Dolt's ref conventions should
// classify branches, remotes, and internal refs as oldGen. Callers with
// custom ref schemes can provide their own classification logic.
//
// If classifier is nil, all refs are treated as newGen (safe but less
// efficient - the GC will do a single-phase collection).
func CollectGarbage(ctx context.Context, db Database, gcConfig chunks.GCConfig, classifier RefClassifier, safepointController types.GCSafepointController) error {
	collector, ok := db.(GarbageCollector)
	if !ok {
		return fmt.Errorf("this database does not support garbage collection")
	}

	datasets, err := db.Datasets(ctx)
	if err != nil {
		return err
	}

	newGen := make(hash.HashSet)
	oldGen := make(hash.HashSet)
	err = datasets.IterAll(ctx, func(datasetID string, h hash.Hash) error {
		if classifier != nil && classifier(datasetID) {
			oldGen.Insert(h)
		} else {
			newGen.Insert(h)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return collector.GC(ctx, gcConfig, oldGen, newGen, safepointController)
}
