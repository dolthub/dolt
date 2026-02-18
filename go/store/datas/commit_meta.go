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

package datas

import (
	"errors"
	"fmt"
	"time"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	commitMetaNameKey      = "name"
	commitMetaEmailKey     = "email"
	commitMetaDescKey      = "desc"
	commitMetaTimestampKey = "timestamp"
	commitMetaUserTSKey    = "user_timestamp"
	commitMetaVersionKey   = "metaversion"
	commitMetaSignature    = "signature"

	commitMetaStName  = "metadata"
	commitMetaVersion = "1.0"
)

const defaultInitialCommitMessage = "Initialize data repository"

var ErrNameNotConfigured = errors.New("Aborting commit due to empty committer name. Is your config set?")
var ErrEmailNotConfigured = errors.New("Aborting commit due to empty committer email. Is your config set?")
var ErrEmptyCommitMessage = errors.New("Aborting commit due to empty commit message.")
var ErrEmptyCommitterName = errors.New("aborting construction of commit metadata, missing committer name in constructor")
var ErrEmptyCommitterEmail = errors.New("aborting construction of commit metadata, missing committer email in constructor")

// CommitterDate is the function used to get the committer time when creating commits. Tests rely on this function to
// produce deterministic hash values and results.
var CommitterDate = time.Now
var CommitLoc = time.Local

// CommitMeta contains all the metadata that is associated with a commit within a data repository to be serialized into
// the database. This does not include control flags for the commit process like [actions.CommitStagedProps].
type CommitMeta struct {
	Name        string
	Email       string
	Description string
	Signature   string
	// Timestamp is the committer date.
	Timestamp *uint64
	// UserTimestamp is the author date. The author date is represented as [int64] which indicates it can represent
	// dates before 1970. When we create a [datas.Commit] object we retrieve and cast the author and committer dates
	// from Timestamp as a result to avoid these out of range values.
	// TODO(elianddb): Config variables can overwrite this behavior, allowing out of range values to be written to
	//  Timestamp. Decide on how to clamp the value.
	UserTimestamp  *int64
	CommitterName  string
	CommitterEmail string
}

// NewCommitMeta creates a CommitMeta instance from a name, email, and description.
func NewCommitMeta(name, email, desc string) (*CommitMeta, error) {
	return NewCommitMetaWithUserTimestamp(name, email, desc, nil)
}

// NewCommitMetaWithUserTimestamp creates a [CommitMeta] object using only the author identity, description and author
// date.
func NewCommitMetaWithUserTimestamp(name, email, desc string, userTimestamp *time.Time) (*CommitMeta, error) {
	return NewCommitMetaWithAuthorCommitter(name, email, desc, userTimestamp, name, email, nil)
}

// NewCommitMetaWithAuthorCommitter creates a [CommitMeta] object using the author and committer identity with the
// option to specify the author and committer dates explicitly.
func NewCommitMetaWithAuthorCommitter(authorName, authorEmail, description string, authorTimestamp *time.Time, committerName, committerEmail string, committerTimestamp *time.Time) (*CommitMeta, error) {
	if authorName == "" {
		return nil, ErrNameNotConfigured
	}

	if authorEmail == "" {
		return nil, ErrEmailNotConfigured
	}

	if description == "" {
		return nil, ErrEmptyCommitMessage
	}

	if committerName == "" {
		return nil, ErrEmptyCommitterName
	}

	if committerEmail == "" {
		return nil, ErrEmptyCommitterEmail
	}

	var committerDateMillis *uint64

	// explicit timestamp overwrite (i.e. when using --date) blocking [datas.CommitterDate()] when creating a new commit
	if committerTimestamp != nil {
		temp := uint64(committerTimestamp.UnixMilli())
		committerDateMillis = &temp
	}

	var authorDateMillis *int64
	if authorTimestamp != nil {
		temp := authorTimestamp.UnixMilli()
		authorDateMillis = &temp
	}

	return &CommitMeta{authorName, authorEmail, description, "", committerDateMillis, authorDateMillis, committerName, committerEmail}, nil
}

func getRequiredFromSt(st types.Struct, k string) (types.Value, error) {
	if v, ok, err := st.MaybeGet(k); err != nil {
		return nil, err
	} else if ok {
		return v, nil
	}

	return nil, errors.New("Missing required field \"" + k + "\".")
}

func CommitMetaFromNomsSt(st types.Struct) (*CommitMeta, error) {
	authorEmail, err := getRequiredFromSt(st, commitMetaEmailKey)

	if err != nil {
		return nil, err
	}

	authorName, err := getRequiredFromSt(st, commitMetaNameKey)

	if err != nil {
		return nil, err
	}

	authorDescription, err := getRequiredFromSt(st, commitMetaDescKey)

	if err != nil {
		return nil, err
	}

	committerTimestamp, err := getRequiredFromSt(st, commitMetaTimestampKey)

	if err != nil {
		return nil, err
	}

	authorTimestamp, ok, err := st.MaybeGet(commitMetaUserTSKey)

	if err != nil {
		return nil, err
	} else if !ok {
		authorTimestamp = types.Int(int64(committerTimestamp.(types.Uint)))
	}

	signature, ok, err := st.MaybeGet(commitMetaSignature)

	if err != nil {
		return nil, err
	} else if !ok {
		signature = types.String("")
	}

	committerDate := uint64(committerTimestamp.(types.Uint))
	authorDate := int64(authorTimestamp.(types.Int))
	return &CommitMeta{
		Name:          string(authorName.(types.String)),
		Email:         string(authorEmail.(types.String)),
		Description:   string(authorDescription.(types.String)),
		Signature:     string(signature.(types.String)),
		Timestamp:     &committerDate,
		UserTimestamp: &authorDate,
		// Committer identity came after Noms storage, so assume these commits have the same author and committer.
		CommitterName:  string(authorName.(types.String)),
		CommitterEmail: string(authorEmail.(types.String)),
	}, nil
}

func (cm *CommitMeta) toNomsStruct(nbf *types.NomsBinFormat) (types.Struct, error) {
	metadata := types.StructData{
		commitMetaNameKey:      types.String(cm.Name),
		commitMetaEmailKey:     types.String(cm.Email),
		commitMetaDescKey:      types.String(cm.Description),
		commitMetaTimestampKey: types.Uint(*cm.Timestamp),
		commitMetaVersionKey:   types.String(commitMetaVersion),
		commitMetaUserTSKey:    types.Int(*cm.UserTimestamp),
		commitMetaSignature:    types.String(cm.Signature),
	}

	return types.NewStruct(nbf, commitMetaStName, metadata)
}

// Time returns the time at which the commit was authored
// This does not preserve timezone information, and returns the time in the system's local timezone
func (cm *CommitMeta) Time() time.Time {
	return time.UnixMilli(*cm.UserTimestamp)
}

// CommitterTime returns the time at which the commit was created
// This does not preserve timezone information, and returns the time in the system's local timezone
func (cm *CommitMeta) CommitterTime() time.Time {
	return time.UnixMilli(int64(*cm.Timestamp))
}

// FormatTS takes the internal timestamp and turns it into a human-readable string in the time.RubyDate format
// which looks like: "Mon Jan 02 15:04:05 -0700 2006"
//
// We round this to the nearest second, which is what MySQL timestamp does by default. This returns the author timestamp
// in the standard Git log format.
func (cm *CommitMeta) FormatTS() string {
	return cm.Time().In(CommitLoc).Round(time.Second).Format(time.RubyDate)
}

// String returns the human readable string representation of the commit data
func (cm *CommitMeta) String() string {
	return fmt.Sprintf("name: %s, email: %s, timestamp: %s, description: %s", cm.Name, cm.Email, cm.FormatTS(), cm.Description)
}

// CommitMetaGenerator is an interface that generates a sequence of CommitMeta structs, and implements a predicate to check whether
// a proposed commit is acceptable.
type CommitMetaGenerator interface {
	Next() (*CommitMeta, error)
	IsGoodCommit(*Commit) bool
}

// The default implementation of CommitMetaGenerator, which generates a single commit which is always acceptable.
type simpleCommitMetaGenerator struct {
	name, email      string
	timestamp        time.Time
	message          string
	alreadyGenerated bool
}

func (g *simpleCommitMetaGenerator) Next() (*CommitMeta, error) {
	if g.alreadyGenerated {
		return nil, fmt.Errorf("Called simpleCommitMetaGenerator.Next twice. This should never happen.")
	}
	g.alreadyGenerated = true
	return NewCommitMetaWithUserTimestamp(g.name, g.email, g.message, &g.timestamp)
}

func (*simpleCommitMetaGenerator) IsGoodCommit(*Commit) bool {
	return true
}

func MakeCommitMetaGenerator(name, email string, timestamp time.Time) CommitMetaGenerator {
	return &simpleCommitMetaGenerator{name: name, email: email, timestamp: timestamp, message: defaultInitialCommitMessage, alreadyGenerated: false}
}

// SignaturePayloadV1 generates the legacy signature payload format that includes only author information.
// This format is used for backward compatibility with commits created before committer metadata was added.
func SignaturePayloadV1(dbName string, meta *CommitMeta, headHash, stagedHash string) string {
	return fmt.Sprintf("db: %s\nMessage: %s\nName: %s\nEmail: %s\nDate: %s\nHead: %s\nStaged: %s",
		dbName,
		meta.Description,
		meta.Name,
		meta.Email,
		meta.Time().String(),
		headHash,
		stagedHash,
	)
}

// SignaturePayloadV2 generates the new signature payload format that includes both author and committer information.
// This format appends committer fields at the end to maintain V1 as a prefix for easier compatibility checks.
func SignaturePayloadV2(dbName string, meta *CommitMeta, headHash, stagedHash string) string {
	return fmt.Sprintf("%s\n CommitterName: %s\nCommitterEmail: %s\nCommitterDate: %s",
		SignaturePayloadV1(dbName, meta, headHash, stagedHash),
		meta.CommitterName,
		meta.CommitterEmail,
		meta.CommitterTime().String())
}
