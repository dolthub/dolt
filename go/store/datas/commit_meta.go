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

// CommitDate represents an optional date for a commit operation. Use [CommitDateNow] to resolve the date at commit
// creation time, or [CommitDateAt] to specify an explicit date (including zero time).
type CommitDate struct {
	t   time.Time
	set bool
}

// CommitDateNow returns a CommitDate that resolves to [CommitterDate]() at the time the commit is created.
func CommitDateNow() CommitDate { return CommitDate{} }

// CommitDateAt returns a CommitDate with the explicitly specified time. The zero time is a valid explicit value.
func CommitDateAt(t time.Time) CommitDate { return CommitDate{t: t, set: true} }

// Resolve returns the concrete time for this CommitDate. If no date was explicitly set, defaultFn is called.
func (d CommitDate) Resolve(defaultFn func() time.Time) time.Time {
	if d.set {
		return d.t
	}
	return defaultFn()
}

// CommitIdentity groups the name, email, and optional date for a single author or committer.
// Use [CommitDateAt] for an explicit date or [CommitDateNow] to resolve at commit time.
type CommitIdentity struct {
	Name  string
	Email string
	Date  CommitDate
}

// CommitMeta contains all the metadata that is associated with a commit within a data repository to be serialized into
// the database. This does not include control flags for the commit process like [actions.CommitStagedProps].
type CommitMeta struct {
	Name        string
	Email       string
	Description string
	Signature   string
	// Timestamp is the committer date. CommitDateNow() is resolved at serialization time by newCommitForValue.
	Timestamp CommitDate
	// UserTimestamp is the author date. CommitDateNow() is resolved at serialization time by newCommitForValue.
	UserTimestamp  CommitDate
	CommitterName  string
	CommitterEmail string
}

// TimestampMillis returns the committer date as milliseconds since Unix epoch.
func (cm *CommitMeta) TimestampMillis() uint64 {
	return uint64(cm.Timestamp.Resolve(CommitterDate).UnixMilli())
}

// UserTimestampMillis returns the author date as milliseconds since Unix epoch.
func (cm *CommitMeta) UserTimestampMillis() int64 {
	return cm.UserTimestamp.Resolve(CommitterDate).UnixMilli()
}

// NewCommitMeta creates a CommitMeta instance from a name, email, and description. Author and committer dates are
// both resolved to [CommitterDate]().
func NewCommitMeta(name, email, desc string) (*CommitMeta, error) {
	identity := CommitIdentity{Name: name, Email: email, Date: CommitDateNow()}
	return NewCommitMetaWithAuthorAndCommitter(identity, identity, desc)
}

// NewCommitMetaWithAuthorDate creates a [CommitMeta] object using only the author identity, description, and an
// explicit author date. The committer identity mirrors the author, with its date resolved to [CommitterDate]().
func NewCommitMetaWithAuthorDate(name, email, desc string, authorDate time.Time) (*CommitMeta, error) {
	author := CommitIdentity{Name: name, Email: email, Date: CommitDateAt(authorDate)}
	committer := CommitIdentity{Name: name, Email: email, Date: CommitDateNow()}
	return NewCommitMetaWithAuthorAndCommitter(author, committer, desc)
}

// NewCommitMetaWithAuthorAndCommitter creates a fully-resolved [CommitMeta] object with distinct author and committer
// identities. [CommitDateNow] for either identity resolves to the same captured timestamp so both are never off by
// different amounts.
func NewCommitMetaWithAuthorAndCommitter(author CommitIdentity, committer CommitIdentity, description string) (*CommitMeta, error) {
	if author.Name == "" {
		return nil, ErrNameNotConfigured
	}
	if author.Email == "" {
		return nil, ErrEmailNotConfigured
	}
	if description == "" {
		return nil, ErrEmptyCommitMessage
	}
	if committer.Name == "" {
		return nil, ErrEmptyCommitterName
	}
	if committer.Email == "" {
		return nil, ErrEmptyCommitterEmail
	}

	// Dates are intentionally not resolved here. newCommitForValue resolves Timestamp and UserTimestamp at
	// serialization time, ensuring both share the same CommitterDate() snapshot when using CommitDateNow().
	return &CommitMeta{
		Name:           author.Name,
		Email:          author.Email,
		Description:    description,
		CommitterName:  committer.Name,
		CommitterEmail: committer.Email,
		Timestamp:      committer.Date,
		UserTimestamp:  author.Date,
	}, nil
}

// Time returns the time at which the commit was authored.
// This does not preserve timezone information, and returns the time in the system's local timezone.
func (cm *CommitMeta) Time() time.Time {
	return cm.UserTimestamp.Resolve(CommitterDate)
}

// CommitterTime returns the time at which the commit was created.
// This does not preserve timezone information, and returns the time in the system's local timezone.
func (cm *CommitMeta) CommitterTime() time.Time {
	return cm.Timestamp.Resolve(CommitterDate)
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
	author := CommitIdentity{Name: g.name, Email: g.email, Date: CommitDateAt(g.timestamp)}
	committer := CommitIdentity{Name: g.name, Email: g.email, Date: CommitDateNow()}
	return NewCommitMetaWithAuthorAndCommitter(author, committer, g.message)
}

func (*simpleCommitMetaGenerator) IsGoodCommit(*Commit) bool {
	return true
}

func MakeCommitMetaGenerator(name, email string, timestamp time.Time) CommitMetaGenerator {
	return &simpleCommitMetaGenerator{name: name, email: email, timestamp: timestamp, message: defaultInitialCommitMessage, alreadyGenerated: false}
}

// signaturePayloadV1 generates the legacy signature payload format that includes only author information.
func signaturePayloadV1(dbName string, meta *CommitMeta, headHash, stagedHash string) string {
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

// SignaturePayloadV2 generates the signature payload including both author and committer information.
// Committer fields are appended after the author fields so V1 is a strict prefix, enabling forward compatibility.
func SignaturePayloadV2(dbName string, meta *CommitMeta, headHash, stagedHash string) string {
	return fmt.Sprintf("%s\nCommitterName: %s\nCommitterEmail: %s\nCommitterDate: %s",
		signaturePayloadV1(dbName, meta, headHash, stagedHash),
		meta.CommitterName,
		meta.CommitterEmail,
		meta.CommitterTime().String())
}
