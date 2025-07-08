// Copyright 2025 Dolthub, Inc.
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

package nbs

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

func MoveNewGenToOldGen(
	ctx context.Context,
	cs chunks.ChunkStore,
	progress chan interface{},
) error {
	if gs, ok := cs.(*GenerationalNBS); ok {
		srcPath, _ := gs.newGen.Path()
		dstPath, _ := gs.oldGen.Path()

		allFiles := make([]hash.Hash, 0, len(gs.newGen.tables.upstream))
		sourceSet := gs.newGen.tables.upstream
		for tf := range sourceSet {
			allFiles = append(allFiles, tf)
		}

		for _, h := range allFiles {
			fileName := h.String()
			arcExists, err := archiveFileExists(ctx, srcPath, h.String())
			if err != nil {
				return err
			}
			if arcExists {
				fileName = fileName + ArchiveFileSuffix
			}

			srcFile := filepath.Join(srcPath, fileName)
			dstFile := filepath.Join(dstPath, fileName)

			progress <- "Moving " + srcFile + " to " + dstFile

			err = nonAtomicSwap(ctx, h, gs.newGen, gs.oldGen, srcFile, dstFile)
			if err != nil {
				return err
			}
		}
	} else {
		return errors.New("runtime error: GenerationalNBS Expected")
	}
	return nil
}

func nonAtomicSwap(ctx context.Context, id hash.Hash, src, dst *NomsBlockStore, srcPath, dstPath string) error {
	srcSpecs, err := src.tables.toSpecs()
	if err != nil {
		return err
	}

	specFound := false
	var movedTs tableSpec
	newSrcSpecs := make([]tableSpec, 0, len(srcSpecs)-1)
	for _, spec := range srcSpecs {
		if id == spec.name {
			movedTs = spec
			specFound = true
		} else {
			newSrcSpecs = append(newSrcSpecs, spec)
		}
	}

	if !specFound {
		return errors.New("table spec not found in source: " + id.String())
	}

	dstSpecs, err := dst.tables.toSpecs()
	if err != nil {
		return err
	}
	newDstSpecs := make([]tableSpec, 0, len(dstSpecs)+1)
	for _, spec := range dstSpecs {
		newDstSpecs = append(newDstSpecs, spec)
	}
	// Add the moved table spec to the destination specs
	newDstSpecs = append(newDstSpecs, movedTs)

	err = src.swapTables(ctx, newSrcSpecs, chunks.GCMode_Default)
	if err != nil {
		return err
	}
	err = os.Rename(srcPath, dstPath)
	if err != nil {
		return err
	}
	err = dst.swapTables(ctx, newDstSpecs, chunks.GCMode_Default)
	if err != nil {
		return err
	}
	return nil
}
