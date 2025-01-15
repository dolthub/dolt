// Copyright 2020 Dolthub, Inc.
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

package actions

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/fatih/color"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
)

type TagProps struct {
	TaggerName  string
	TaggerEmail string
	Description string
}

func CreateTag(ctx context.Context, dEnv *env.DoltEnv, tagName, startPoint string, props TagProps) error {
	headRef, err := dEnv.RepoStateReader().CWBHeadRef()
	if err != nil {
		return err
	}
	return CreateTagOnDB(ctx, dEnv.DoltDB, tagName, startPoint, props, headRef)
}

func CreateTagOnDB(ctx context.Context, ddb *doltdb.DoltDB, tagName, startPoint string, props TagProps, headRef ref.DoltRef) error {
	tagRef := ref.NewTagRef(tagName)

	hasRef, err := ddb.HasRef(ctx, tagRef)

	if err != nil {
		return err
	}

	if hasRef {
		return ErrAlreadyExists
	}

	if !ref.IsValidTagName(tagName) {
		return doltdb.ErrInvTagName
	}

	cs, err := doltdb.NewCommitSpec(startPoint)
	if err != nil {
		return err
	}

	optCmt, err := ddb.Resolve(ctx, cs, headRef)
	if err != nil {
		return err
	}
	cm, ok := optCmt.ToCommit()
	if !ok {
		return doltdb.ErrGhostCommitEncountered
	}

	meta := datas.NewTagMeta(props.TaggerName, props.TaggerEmail, props.Description)

	return ddb.NewTagAtCommit(ctx, tagRef, cm, meta)
}

func DeleteTagsOnDB(ctx context.Context, ddb *doltdb.DoltDB, tagNames ...string) error {
	for _, tn := range tagNames {
		dref := ref.NewTagRef(tn)

		hasRef, err := ddb.HasRef(ctx, dref)

		if err != nil {
			return err
		}
		if !hasRef {
			return doltdb.ErrTagNotFound
		}

		err = ddb.DeleteTag(ctx, dref)

		if err != nil {
			return err
		}
	}
	return nil
}

// IterResolvedTags iterates over tags in dEnv.DoltDB from newest to oldest, resolving the tag to a commit and calling cb().
func IterResolvedTags(ctx context.Context, ddb *doltdb.DoltDB, cb func(tag *doltdb.Tag) (stop bool, err error)) error {
	startGetTags := time.Now()
	tagRefs, err := ddb.GetTags(ctx)

	if err != nil {
		fmt.Fprintf(color.Output, "DUSTIN: IterResolvedTags: ddb.GetTags: error: elapsed: %v\n", time.Since(startGetTags))
		return err
	}
	fmt.Fprintf(color.Output, "DUSTIN: IterResolvedTags: ddb.GetTags: success: elapsed: %v\n", time.Since(startGetTags))

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(128)

	startResolveTags := time.Now()
	resolved := make([]*doltdb.Tag, len(tagRefs))
	for idx, r := range tagRefs {
		idx, r := idx, r
		eg.Go(func() error {
			if egCtx.Err() != nil {
				return egCtx.Err()
			}

			tr, ok := r.(ref.TagRef)
			if !ok {
				fmt.Fprintf(color.Output, "DUSTIN: IterResolvedTags: ref.TagRef conv: error: elapsed: %v\n", time.Since(startResolveTags))
				return fmt.Errorf("DoltDB.GetTags() returned non-tag DoltRef")
			}

			tag, err := ddb.ResolveTag(egCtx, tr)
			if err != nil {
				fmt.Fprintf(color.Output, "DUSTIN: IterResolvedTags: resolve tag: error: elapsed: %v\n", time.Since(startResolveTags))
				return err
			}

			resolved[idx] = tag
			return nil
		})
	}

	err = eg.Wait()
	if err != nil {
		return err
	}

	fmt.Fprintf(color.Output, "DUSTIN: IterResolvedTags: resolve tags: success: elapsed: %v\n", time.Since(startResolveTags))

	// iterate newest to oldest
	sort.Slice(resolved, func(i, j int) bool {
		return resolved[i].Meta.Timestamp > resolved[j].Meta.Timestamp
	})

	startCB := time.Now()
	for _, tag := range resolved {
		stop, err := cb(tag)

		if err != nil {
			fmt.Fprintf(color.Output, "DUSTIN: IterResolvedTags: cb: error: elapsed: %v\n", time.Since(startCB))
			return err
		}
		if stop {
			break
		}
	}

	fmt.Fprintf(color.Output, "DUSTIN: IterResolvedTags: cb: success: elapsed: %v\n", time.Since(startCB))

	return nil
}
