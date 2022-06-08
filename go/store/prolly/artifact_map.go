// Copyright 2022 Dolthub, Inc.
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

package prolly

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type ArtifactType string

const (
	// ArtifactTypeConflict is the type for conflicts.
	ArtifactTypeConflict ArtifactType = "conflict"
	// ArtifactTypeForeignKeyViol is the type for foreign key violations.
	ArtifactTypeForeignKeyViol = "fk_viol"
	// ArtifactTypeUniqueKeyViol is the type for unique key violations.
	ArtifactTypeUniqueKeyViol = "unique_viol"
	// ArtifactTypeChkConsViol is the type for check constraint violations.
	ArtifactTypeChkConsViol = "chk_viol"
)

type ArtifactMap struct {
	tuples orderedTree[val.Tuple, val.Tuple, val.TupleDesc]
	// the description of the source table where these artifacts come from
	srcKeyDesc val.TupleDesc
	keyDesc    val.TupleDesc
	valDesc    val.TupleDesc
}

// NewArtifactMap creates an artifact map based on |srcKeyDesc| which is the key descriptor for
// the corresponding row map.
func NewArtifactMap(node tree.Node, ns tree.NodeStore, srcKeyDesc val.TupleDesc) ArtifactMap {
	keyDesc, valDesc := calcArtifactsDescriptors(srcKeyDesc)
	tuples := orderedTree[val.Tuple, val.Tuple, val.TupleDesc]{
		root:  node,
		ns:    ns,
		order: keyDesc,
	}
	return ArtifactMap{
		tuples:     tuples,
		srcKeyDesc: srcKeyDesc,
		keyDesc:    keyDesc,
		valDesc:    valDesc,
	}
}

// NewArtifactMapFromTuples creates an artifact map based on |srcKeyDesc| which is the key descriptor for
// the corresponding row map and inserts the given |tups|. |tups| must be a key followed by a value.
func NewArtifactMapFromTuples(ctx context.Context, ns tree.NodeStore, srcKeyDesc val.TupleDesc, tups ...val.Tuple) (ArtifactMap, error) {
	serializer := message.ProllyMapSerializer{Pool: ns.Pool()}
	ch, err := tree.NewEmptyChunker(ctx, ns, serializer)
	if err != nil {
		return ArtifactMap{}, err
	}

	if len(tups)%2 != 0 {
		return ArtifactMap{}, fmt.Errorf("tuples must be key-value pairs")
	}

	for i := 0; i < len(tups); i += 2 {
		if err = ch.AddPair(ctx, tree.Item(tups[i]), tree.Item(tups[i+1])); err != nil {
			return ArtifactMap{}, err
		}
	}

	root, err := ch.Done(ctx)
	if err != nil {
		return ArtifactMap{}, err
	}

	return NewArtifactMap(root, ns, srcKeyDesc), nil
}

func (m ArtifactMap) Count() int {
	return m.tuples.count()
}

func (m ArtifactMap) Height() int {
	return m.tuples.height()
}

func (m ArtifactMap) HashOf() hash.Hash {
	return m.tuples.hashOf()
}

func (m ArtifactMap) Node() tree.Node {
	return m.tuples.root
}

func (m ArtifactMap) Format() *types.NomsBinFormat {
	return m.tuples.ns.Format()
}

func (m ArtifactMap) Descriptors() (key, val val.TupleDesc) {
	return m.keyDesc, m.valDesc
}

func (m ArtifactMap) WalkAddresses(ctx context.Context, cb tree.AddressCb) error {
	return m.tuples.walkAddresses(ctx, cb)
}

func (m ArtifactMap) WalkNodes(ctx context.Context, cb tree.NodeCb) error {
	return m.tuples.walkNodes(ctx, cb)
}

func (m ArtifactMap) Get(ctx context.Context, key val.Tuple, cb KeyValueFn[val.Tuple, val.Tuple]) (err error) {
	return m.tuples.get(ctx, key, cb)
}

func (m ArtifactMap) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	return m.tuples.has(ctx, key)
}

func (m ArtifactMap) Pool() pool.BuffPool {
	return m.tuples.ns.Pool()
}

func (m ArtifactMap) Editor() ArtifactsEditor {
	return ArtifactsEditor{
		srcKeyDesc: m.srcKeyDesc,
		mut: MutableMap{
			tuples:  m.tuples.mutate(),
			keyDesc: m.keyDesc,
			valDesc: m.valDesc,
		},
	}
}

// IterAll returns an iterator for all artifacts.
func (m ArtifactMap) IterAll(ctx context.Context) (ArtifactIter, error) {
	numPks := m.srcKeyDesc.Count()
	tb := val.NewTupleBuilder(m.srcKeyDesc)
	itr, err := m.tuples.iterAll(ctx)
	if err != nil {
		return ArtifactIter{}, err
	}
	return ArtifactIter{
		itr:    itr,
		numPks: numPks,
		tb:     tb,
		pool:   m.Pool(),
		artKD:  m.keyDesc,
		artVD:  m.valDesc,
	}, nil
}

// IterAllConflicts returns an iterator for the conflicts.
func (m ArtifactMap) IterAllConflicts(ctx context.Context) (ConflictArtifactIter, error) {
	artIter, err := m.iterAllOfType(ctx, ArtifactTypeConflict)
	if err != nil {
		return ConflictArtifactIter{}, err
	}

	return ConflictArtifactIter{artIter}, nil
}

// HasArtifactOfType returns whether an artifact of |artType| exists in the map.
func (m ArtifactMap) HasArtifactOfType(ctx context.Context, artType ArtifactType) (bool, error) {
	artIter, err := m.iterAllOfType(ctx, artType)
	if err != nil {
		return false, err
	}

	_, err = artIter.Next(ctx)
	if err != nil && err != io.EOF {
		return false, err
	}

	// err is either nil or io.EOF
	hasType := err == nil
	return hasType, nil
}

// ClearArtifactsOfType deletes all artifacts of |artType|.
func (m ArtifactMap) ClearArtifactsOfType(ctx context.Context, artType ArtifactType) (ArtifactMap, error) {
	edt := m.Editor()
	itr, err := m.iterAllOfType(ctx, artType)
	if err != nil {
		return ArtifactMap{}, err
	}
	var art Artifact
	for {
		art, err = itr.Next(ctx)
		if err != nil && err != io.EOF {
			return ArtifactMap{}, err
		}
		if err == io.EOF {
			break
		}

		dErr := edt.Delete(ctx, art.ArtKey)
		if dErr != nil {
			return ArtifactMap{}, dErr
		}
	}

	return edt.Flush(ctx)
}

// CountOfType returns the number of artifacts of |artType|.
func (m ArtifactMap) CountOfType(ctx context.Context, artType ArtifactType) (cnt uint64, err error) {
	itr, err := m.iterAllOfType(ctx, artType)
	if err != nil {
		return 0, err
	}
	for err != io.EOF {
		_, err = itr.Next(ctx)
		if err != nil && err != io.EOF {
			return 0, err
		}
		cnt++
	}
	return cnt, nil
}

func (m ArtifactMap) iterAllOfType(ctx context.Context, artType ArtifactType) (artifactTypeIter, error) {
	itr, err := m.IterAll(ctx)
	if err != nil {
		return artifactTypeIter{}, err
	}
	return artifactTypeIter{itr, artType}, nil
}

func MergeArtifactMaps(ctx context.Context, left, right, base ArtifactMap, cb tree.CollisionFn) (ArtifactMap, error) {
	serializer := message.ProllyMapSerializer{Pool: left.tuples.ns.Pool()}
	tuples, err := mergeOrderedTrees(ctx, left.tuples, right.tuples, base.tuples, cb, serializer)
	if err != nil {
		return ArtifactMap{}, err
	}

	return ArtifactMap{
		tuples:  tuples,
		keyDesc: base.keyDesc,
		valDesc: base.valDesc,
	}, nil
}

type ArtifactsEditor struct {
	mut        MutableMap
	srcKeyDesc val.TupleDesc
}

func (wr ArtifactsEditor) Add(ctx context.Context, key val.Tuple, val val.Tuple) error {
	return wr.mut.Put(ctx, key, val)
}

func (wr ArtifactsEditor) Delete(ctx context.Context, key val.Tuple) error {
	return wr.mut.Delete(ctx, key)
}

func (wr ArtifactsEditor) Flush(ctx context.Context) (ArtifactMap, error) {
	m, err := wr.mut.Map(ctx)
	if err != nil {
		return ArtifactMap{}, err
	}

	return ArtifactMap{
		tuples:     m.tuples,
		srcKeyDesc: wr.srcKeyDesc,
		keyDesc:    wr.mut.keyDesc,
		valDesc:    wr.mut.valDesc,
	}, nil
}

// ConflictArtifactIter iters all the conflicts in ArtifactMap.
type ConflictArtifactIter struct {
	itr artifactTypeIter
}

func (itr *ConflictArtifactIter) Next(ctx context.Context) (ConflictArtifact, error) {
	art, err := itr.itr.Next(ctx)
	if err != nil {
		return ConflictArtifact{}, err
	}

	var parsedMeta ConflictMetadata
	err = json.Unmarshal(art.Metadata, &parsedMeta)
	if err != nil {
		return ConflictArtifact{}, err
	}

	return ConflictArtifact{
		Key:        art.Key,
		HeadCmHash: art.HeadCmHash,
		Metadata:   parsedMeta,
	}, nil
}

// ConflictArtifact is the decoded conflict from the artifacts table
type ConflictArtifact struct {
	Key        val.Tuple
	HeadCmHash []byte
	Metadata   ConflictMetadata
}

// ConflictMetadata is the json metadata associated with a conflict
type ConflictMetadata struct {
	// BaseTblHash is the target hash of the table holding the base value for the conflict
	BaseTblHash []byte `json:"bc"`
	// TheirTblHash is the target hash of the table holding the their value for the conflict
	TheirTblHash []byte `json:"tc"`
}

// artifactTypeIter iters all artifacts of a given |artType|.
type artifactTypeIter struct {
	itr     ArtifactIter
	artType ArtifactType
}

func (itr artifactTypeIter) Next(ctx context.Context) (art Artifact, err error) {
	for art.ArtType != itr.artType {
		art, err = itr.itr.Next(ctx)
		if err != nil {
			return Artifact{}, err
		}
	}

	return art, nil
}

// ArtifactIter iterates artifacts as a decoded artifact struct.
type ArtifactIter struct {
	itr          MapIter
	artKD, artVD val.TupleDesc
	tb           *val.TupleBuilder
	pool         pool.BuffPool
	numPks       int
}

func (itr ArtifactIter) Next(ctx context.Context) (Artifact, error) {
	artKey, v, err := itr.itr.Next(ctx)
	if err != nil {
		return Artifact{}, err
	}

	srcKey := itr.getSrcKeyFromArtKey(artKey)
	cmHash, _ := itr.artKD.GetHash160(itr.numPks, artKey)
	artType, _ := itr.artKD.GetString(itr.numPks+1, artKey)
	metadata, _ := itr.artVD.GetJSON(0, v)

	return Artifact{
		ArtKey:     artKey,
		Key:        srcKey,
		HeadCmHash: cmHash,
		ArtType:    ArtifactType(artType),
		Metadata:   metadata,
	}, nil
}

func (itr ArtifactIter) getSrcKeyFromArtKey(k val.Tuple) val.Tuple {
	for i := 0; i < itr.numPks; i++ {
		itr.tb.PutRaw(0, k.GetField(i))
	}
	return itr.tb.Build(itr.pool)
}

// Artifact is a struct representing an artifact in the artifacts table
type Artifact struct {
	// ArtKey is the key of the artifact itself
	ArtKey val.Tuple
	// Key is the key of the source row that the artifact references
	Key val.Tuple
	// HeadCmHash is the cm hash of the left branch's head at the time of artifact creation
	HeadCmHash []byte
	// ArtType is the type of the artifact
	ArtType ArtifactType
	// Metadata is the encoded json metadata
	Metadata []byte
}

func calcArtifactsDescriptors(srcKd val.TupleDesc) (kd, vd val.TupleDesc) {

	// artifact key consists of keys of source schema, followed by target branch
	// commit hash, and artifact type.
	keyTypes := srcKd.Types

	// target branch commit hash
	keyTypes = append(keyTypes, val.Type{Enc: val.Hash160Enc, Nullable: false})

	// artifact type
	keyTypes = append(keyTypes, val.Type{Enc: val.StringEnc, Nullable: false})

	// json blob data
	valTypes := []val.Type{{Enc: val.JSONEnc, Nullable: false}}

	return val.NewTupleDescriptor(keyTypes...), val.NewTupleDescriptor(valTypes...)
}
