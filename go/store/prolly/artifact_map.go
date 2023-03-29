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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type ArtifactType uint8

const (
	// ArtifactTypeConflict is the type for conflicts.
	ArtifactTypeConflict ArtifactType = iota + 1
	// ArtifactTypeForeignKeyViol is the type for foreign key violations.
	ArtifactTypeForeignKeyViol
	// ArtifactTypeUniqueKeyViol is the type for unique key violations.
	ArtifactTypeUniqueKeyViol
	// ArtifactTypeChkConsViol is the type for check constraint violations.
	ArtifactTypeChkConsViol
	artifactMapPendingBufferSize = 650_000
)

type ArtifactMap struct {
	tuples tree.StaticMap[val.Tuple, val.Tuple, val.TupleDesc]
	// the description of the source table where these artifacts come from
	srcKeyDesc val.TupleDesc
	keyDesc    val.TupleDesc
	valDesc    val.TupleDesc
}

// NewArtifactMap creates an artifact map based on |srcKeyDesc| which is the key descriptor for
// the corresponding row map.
func NewArtifactMap(node tree.Node, ns tree.NodeStore, srcKeyDesc val.TupleDesc) ArtifactMap {
	keyDesc, valDesc := mergeArtifactsDescriptorsFromSource(srcKeyDesc)
	tuples := tree.StaticMap[val.Tuple, val.Tuple, val.TupleDesc]{
		Root:      node,
		NodeStore: ns,
		Order:     keyDesc,
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
	kd, vd := mergeArtifactsDescriptorsFromSource(srcKeyDesc)
	serializer := message.NewMergeArtifactSerializer(kd, ns.Pool())

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

	tuples := tree.StaticMap[val.Tuple, val.Tuple, val.TupleDesc]{
		Root:      root,
		NodeStore: ns,
		Order:     kd,
	}
	return ArtifactMap{
		tuples:     tuples,
		srcKeyDesc: srcKeyDesc,
		keyDesc:    kd,
		valDesc:    vd,
	}, nil
}

func (m ArtifactMap) Count() (int, error) {
	return m.tuples.Count()
}

func (m ArtifactMap) Height() int {
	return m.tuples.Height()
}

func (m ArtifactMap) HashOf() hash.Hash {
	return m.tuples.HashOf()
}

func (m ArtifactMap) Node() tree.Node {
	return m.tuples.Root
}

func (m ArtifactMap) NodeStore() tree.NodeStore {
	return m.tuples.NodeStore
}

func (m ArtifactMap) Format() *types.NomsBinFormat {
	return m.tuples.NodeStore.Format()
}

func (m ArtifactMap) Descriptors() (key, val val.TupleDesc) {
	return m.keyDesc, m.valDesc
}

func (m ArtifactMap) WalkAddresses(ctx context.Context, cb tree.AddressCb) error {
	return m.tuples.WalkAddresses(ctx, cb)
}

func (m ArtifactMap) WalkNodes(ctx context.Context, cb tree.NodeCb) error {
	return m.tuples.WalkNodes(ctx, cb)
}

func (m ArtifactMap) Get(ctx context.Context, key val.Tuple, cb tree.KeyValueFn[val.Tuple, val.Tuple]) (err error) {
	return m.tuples.Get(ctx, key, cb)
}

func (m ArtifactMap) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	return m.tuples.Has(ctx, key)
}

func (m ArtifactMap) Pool() pool.BuffPool {
	return m.tuples.NodeStore.Pool()
}

func (m ArtifactMap) Editor() *ArtifactsEditor {
	artKD, artVD := m.Descriptors()
	return &ArtifactsEditor{
		srcKeyDesc: m.srcKeyDesc,
		mut: MutableMap{
			tuples:     m.tuples.Mutate(),
			keyDesc:    m.keyDesc,
			valDesc:    m.valDesc,
			maxPending: artifactMapPendingBufferSize,
		},
		artKB: val.NewTupleBuilder(artKD),
		artVB: val.NewTupleBuilder(artVD),
		pool:  m.Pool(),
	}
}

// IterAll returns an iterator for all artifacts.
func (m ArtifactMap) IterAll(ctx context.Context) (ArtifactIter, error) {
	numPks := m.srcKeyDesc.Count()
	tb := val.NewTupleBuilder(m.srcKeyDesc)
	itr, err := m.tuples.IterAll(ctx)
	if err != nil {
		return nil, err
	}
	return artifactIterImpl{
		itr:    itr,
		numPks: numPks,
		tb:     tb,
		pool:   m.Pool(),
		artKD:  m.keyDesc,
		artVD:  m.valDesc,
	}, nil
}

func (m ArtifactMap) IterAllCVs(ctx context.Context) (ArtifactIter, error) {
	itr, err := m.iterAllOfTypes(ctx, ArtifactTypeForeignKeyViol, ArtifactTypeUniqueKeyViol, ArtifactTypeChkConsViol)
	if err != nil {
		return nil, err
	}
	return itr, nil
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
	itr, err := m.iterAllOfTypes(ctx, artType)
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
	for _, err = itr.Next(ctx); err == nil; _, err = itr.Next(ctx) {
		cnt++
	}
	if err != io.EOF {
		return 0, err
	}
	return cnt, nil
}

// CountOfTypes returns the number of artifacts that match any type in |artTypes|.
func (m ArtifactMap) CountOfTypes(ctx context.Context, artTypes ...ArtifactType) (cnt uint64, err error) {
	itr, err := m.iterAllOfTypes(ctx, artTypes...)
	if err != nil {
		return 0, err
	}
	for _, err = itr.Next(ctx); err == nil; _, err = itr.Next(ctx) {
		cnt++
	}
	if err != io.EOF {
		return 0, err
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

func (m ArtifactMap) iterAllOfTypes(ctx context.Context, artTypes ...ArtifactType) (multiArtifactTypeItr, error) {
	itr, err := m.IterAll(ctx)
	if err != nil {
		return multiArtifactTypeItr{}, err
	}
	return newMultiArtifactTypeItr(itr, artTypes), nil
}

func MergeArtifactMaps(ctx context.Context, left, right, base ArtifactMap, cb tree.CollisionFn) (ArtifactMap, error) {
	serializer := message.NewMergeArtifactSerializer(base.keyDesc, left.tuples.NodeStore.Pool())
	tuples, _, err := tree.MergeOrderedTrees(ctx, left.tuples, right.tuples, base.tuples, cb, serializer)
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
	mut          MutableMap
	srcKeyDesc   val.TupleDesc
	artKB, artVB *val.TupleBuilder
	pool         pool.BuffPool
}

func (wr *ArtifactsEditor) Add(ctx context.Context, srcKey val.Tuple, theirRootIsh hash.Hash, artType ArtifactType, meta []byte) error {
	for i := 0; i < srcKey.Count(); i++ {
		wr.artKB.PutRaw(i, srcKey.GetField(i))
	}
	wr.artKB.PutCommitAddr(srcKey.Count(), theirRootIsh)
	wr.artKB.PutUint8(srcKey.Count()+1, uint8(artType))
	key := wr.artKB.Build(wr.pool)

	wr.artVB.PutJSON(0, meta)
	value := wr.artVB.Build(wr.pool)

	return wr.mut.Put(ctx, key, value)
}

type ErrMergeArtifactCollision struct {
	Key, Val              val.Tuple
	ExistingInfo, NewInfo []byte
}

func (e *ErrMergeArtifactCollision) Error() string {
	return "an existing row was found with different violation info json"
}

// ReplaceConstraintViolation replaces constraint violations that match the
// given one but have a different commit hash. If no existing violation exists,
// the given will be inserted. Returns true if a violation was replaced. If an
// existing violation exists but has a different |meta.VInfo| value then
// ErrMergeArtifactCollision is a returned.
func (wr *ArtifactsEditor) ReplaceConstraintViolation(ctx context.Context, srcKey val.Tuple, theirRootIsh hash.Hash, artType ArtifactType, meta ConstraintViolationMeta) error {
	itr, err := wr.mut.IterRange(ctx, PrefixRange(srcKey, wr.srcKeyDesc))
	if err != nil {
		return err
	}
	aItr := artifactIterImpl{
		itr:    itr,
		artKD:  wr.mut.keyDesc,
		artVD:  wr.mut.valDesc,
		pool:   wr.pool,
		tb:     val.NewTupleBuilder(wr.srcKeyDesc),
		numPks: wr.srcKeyDesc.Count(),
	}

	var art Artifact
	var currMeta ConstraintViolationMeta
	for art, err = aItr.Next(ctx); err == nil; art, err = aItr.Next(ctx) {
		// prefix scanning sometimes returns keys not in the range
		if bytes.Compare(art.Key, srcKey) != 0 {
			continue
		}
		if art.ArtType != artType {
			continue
		}

		err = json.Unmarshal(art.Metadata, &currMeta)
		if err != nil {
			return err
		}

		if bytes.Compare(currMeta.Value, meta.Value) == 0 {
			if bytes.Compare(currMeta.VInfo, meta.VInfo) != 0 {
				return &ErrMergeArtifactCollision{
					Key:          srcKey,
					Val:          currMeta.Value,
					ExistingInfo: currMeta.VInfo,
					NewInfo:      meta.VInfo,
				}
			}
			// Key and Value is the same, so delete this
			err = wr.Delete(ctx, art.ArtKey)
			if err != nil {
				return err
			}
		}
	}
	if err != io.EOF {
		return err
	}

	d, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	err = wr.Add(ctx, srcKey, theirRootIsh, artType, d)
	if err != nil {
		return err
	}

	return nil
}

func (wr *ArtifactsEditor) Delete(ctx context.Context, key val.Tuple) error {
	return wr.mut.Delete(ctx, key)
}

func (wr *ArtifactsEditor) Flush(ctx context.Context) (ArtifactMap, error) {
	s := message.NewMergeArtifactSerializer(wr.artKB.Desc, wr.NodeStore().Pool())

	m, err := wr.mut.flushWithSerializer(ctx, s)
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

func (wr *ArtifactsEditor) NodeStore() tree.NodeStore {
	return wr.mut.NodeStore()
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
		Key:          art.Key,
		TheirRootIsh: art.TheirRootIsh,
		Metadata:     parsedMeta,
	}, nil
}

// ConflictArtifact is the decoded conflict from the artifacts table
type ConflictArtifact struct {
	Key          val.Tuple
	TheirRootIsh hash.Hash
	Metadata     ConflictMetadata
}

// ConflictMetadata is the json metadata associated with a conflict
type ConflictMetadata struct {
	// BaseRootIsh is the target hash of the working set holding the base value for the conflict.
	BaseRootIsh hash.Hash `json:"bc"`
}

// ConstraintViolationMeta is the json metadata for foreign key constraint violations
type ConstraintViolationMeta struct {
	// marshalled json information about the fk
	VInfo []byte `json:"v_info"`
	// value for the violating row
	Value []byte `json:"value"`
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

// multiArtifactTypeItr iters all artifacts of its member types.
type multiArtifactTypeItr struct {
	itr     ArtifactIter
	members []bool
}

var _ ArtifactIter = multiArtifactTypeItr{}

// newMultiArtifactTypeItr creates an iter that iterates an artifact if its type exists in |types|.
func newMultiArtifactTypeItr(itr ArtifactIter, types []ArtifactType) multiArtifactTypeItr {
	members := make([]bool, 5)
	for _, t := range types {
		members[uint8(t)] = true
	}
	return multiArtifactTypeItr{itr, members}
}

func (itr multiArtifactTypeItr) Next(ctx context.Context) (art Artifact, err error) {
	for !itr.members[art.ArtType] {
		art, err = itr.itr.Next(ctx)
		if err != nil {
			return Artifact{}, err
		}
	}

	return art, nil
}

type ArtifactIter interface {
	Next(ctx context.Context) (Artifact, error)
}

// ArtifactIter iterates artifacts as a decoded artifact struct.
type artifactIterImpl struct {
	itr          MapIter
	artKD, artVD val.TupleDesc
	tb           *val.TupleBuilder
	pool         pool.BuffPool
	numPks       int
}

var _ ArtifactIter = artifactIterImpl{}

func (itr artifactIterImpl) Next(ctx context.Context) (Artifact, error) {
	artKey, v, err := itr.itr.Next(ctx)
	if err != nil {
		return Artifact{}, err
	}

	srcKey := itr.getSrcKeyFromArtKey(artKey)
	cmHash, _ := itr.artKD.GetCommitAddr(itr.numPks, artKey)
	artType, _ := itr.artKD.GetUint8(itr.numPks+1, artKey)
	metadata, _ := itr.artVD.GetJSON(0, v)

	return Artifact{
		ArtKey:       artKey,
		Key:          srcKey,
		TheirRootIsh: cmHash,
		ArtType:      ArtifactType(artType),
		Metadata:     metadata,
	}, nil
}

func (itr artifactIterImpl) getSrcKeyFromArtKey(k val.Tuple) val.Tuple {
	for i := 0; i < itr.numPks; i++ {
		itr.tb.PutRaw(i, k.GetField(i))
	}
	return itr.tb.Build(itr.pool)
}

// Artifact is a struct representing an artifact in the artifacts table
type Artifact struct {
	// ArtKey is the key of the artifact itself
	ArtKey val.Tuple
	// Key is the key of the source row that the artifact references
	Key val.Tuple
	// TheirRootIsh is the working set hash or commit hash of the right in the merge
	TheirRootIsh hash.Hash
	// ArtType is the type of the artifact
	ArtType ArtifactType
	// Metadata is the encoded json metadata
	Metadata []byte
}

func mergeArtifactsDescriptorsFromSource(srcKd val.TupleDesc) (kd, vd val.TupleDesc) {

	// artifact key consists of keys of source schema, followed by target branch
	// commit hash, and artifact type.
	keyTypes := srcKd.Types

	// source branch commit hash
	keyTypes = append(keyTypes, val.Type{Enc: val.CommitAddrEnc, Nullable: false})

	// artifact type
	keyTypes = append(keyTypes, val.Type{Enc: val.Uint8Enc, Nullable: false})

	// json blob data
	valTypes := []val.Type{{Enc: val.JSONEnc, Nullable: false}}

	return val.NewTupleDescriptor(keyTypes...), val.NewTupleDescriptor(valTypes...)
}

func ArtifactDebugFormat(ctx context.Context, m ArtifactMap) (string, error) {
	kd, vd := m.Descriptors()
	iter, err := m.tuples.IterAll(ctx)
	if err != nil {
		return "", err
	}
	c, err := m.Count()
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Artifact Map (count: %d) {\n", c))
	for {
		k, v, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		sb.WriteString("\t")
		sb.WriteString(kd.Format(k))
		sb.WriteString(": ")
		sb.WriteString(vd.Format(v))
		sb.WriteString(",\n")
	}
	sb.WriteString("}")
	return sb.String(), nil
}
