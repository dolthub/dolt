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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
)

const DefaultPageSize = 100

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

// IterUnresolvedTags iterates over tags in dEnv.DoltDB, and calls cb() for each with an unresolved Tag.
func IterUnresolvedTags(ctx context.Context, ddb *doltdb.DoltDB, cb func(tag *doltdb.TagResolver) (stop bool, err error)) error {
	tagRefs, err := ddb.GetTags(ctx)
	if err != nil {
		return err
	}

	tagResolvers, err := ddb.GetTagResolvers(ctx, tagRefs)
	if err != nil {
		return err
	}

	for _, tagResolver := range tagResolvers {
		stop, err := cb(&tagResolver)
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}
	return nil
}

// IterResolvedTagsPaginated iterates over tags in dEnv.DoltDB in their default lexicographical order, resolving the tag to a commit and calling cb().
// Returns the next tag name if there are more results available.
func IterResolvedTagsPaginated(ctx context.Context, ddb *doltdb.DoltDB, startTag string, cb func(tag *doltdb.Tag) (stop bool, err error)) (string, error) {
	// tags returned here are sorted lexicographically
	tagRefs, err := ddb.GetTags(ctx)
	if err != nil {
		return "", err
	}

	// find starting index based on start tag
	startIdx := 0
	if startTag != "" {
		for i, tr := range tagRefs {
			if tr.GetPath() == startTag {
				startIdx = i + 1 // start after the given tag
				break
			}
		}
	}

	// get page of results
	endIdx := startIdx + DefaultPageSize
	if endIdx > len(tagRefs) {
		endIdx = len(tagRefs)
	}

	pageTagRefs := tagRefs[startIdx:endIdx]

	// resolve tags for this page
	for _, tr := range pageTagRefs {
		tag, err := ddb.ResolveTag(ctx, tr.(ref.TagRef))
		if err != nil {
			return "", err
		}

		stop, err := cb(tag)
		if err != nil {
			return "", err
		}

		if stop {
			break
		}
	}

	// return next tag name if there are more results
	if endIdx < len(tagRefs) {
		lastTag := pageTagRefs[len(pageTagRefs)-1]
		return lastTag.GetPath(), nil
	}

	return "", nil
}

// VisitResolvedTag iterates over tags in ddb until the given tag name is found, then calls cb() with the resolved tag.
func VisitResolvedTag(ctx context.Context, ddb *doltdb.DoltDB, tagName string, cb func(tag *doltdb.Tag) error) error {
	tagRefs, err := ddb.GetTags(ctx)
	if err != nil {
		return err
	}

	for _, r := range tagRefs {
		tr, ok := r.(ref.TagRef)
		if !ok {
			return fmt.Errorf("DoltDB.GetTags() returned non-tag DoltRef")
		}

		if tr.GetPath() == tagName {
			tag, err := ddb.ResolveTag(ctx, tr)
			if err != nil {
				return err
			}
			return cb(tag)
		}
	}

	return doltdb.ErrTagNotFound
}
