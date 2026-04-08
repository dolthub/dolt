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

	errors2 "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
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

var ErrNameNotConfigured = errors2.NewKind("Aborting commit due to empty %s name. Is your config set?")
var ErrEmailNotConfigured = errors2.NewKind("Aborting commit due to empty %s email. Is your config set?")
var ErrEmptyCommitMessage = errors.New("Aborting commit due to empty commit message.")

// CommitterDate is the function used to get the committer time when creating commits. Tests can replace
// this function to produce deterministic commit hashes.
var CommitterDate = time.Now

// CommitLoc is the location used when formatting commit timestamps for display.
var CommitLoc = time.Local

// CommitDate holds a timestamp for a commit author or committer. Use [CommitDateNow] to capture the
// current time when the commit is written to storage, or use [CommitDateAt] to specify an explicit
// timestamp. The zero time is a valid explicit value.
type CommitDate struct {
	t   time.Time
	set bool
}

// CommitDateNow returns a [CommitDate] that captures the current time when the commit is written to
// storage. The time is obtained by calling [CommitterDate], which tests can override for deterministic results.
func CommitDateNow() CommitDate { return CommitDate{} }

// CommitDateAt returns a [CommitDate] with the given explicit time. The zero time is a valid value.
func CommitDateAt(t time.Time) CommitDate { return CommitDate{t: t, set: true} }

// FreezeAt returns a [CommitDate] with t as the explicit time when d has no explicit time set.
// If d already has an explicit time, FreezeAt returns d unchanged.
func (d CommitDate) FreezeAt(t time.Time) CommitDate {
	if d.set {
		return d
	}
	return CommitDateAt(t)
}

// resolve returns the time for this CommitDate. When no explicit time is set, it calls [CommitterDate].
func (d CommitDate) resolve() time.Time {
	if d.set {
		return d.t
	}
	return CommitterDate()
}

// Time returns the underlying timestamp, calling [CommitterDate] if no explicit time was set.
func (d CommitDate) Time() time.Time { return d.resolve() }

// NewCommitDate parses |dateStr| and returns a [CommitDateAt]. If |dateStr| is empty or cannot be
// parsed, it returns [CommitDateNow] and an error.
func NewCommitDate(dateStr string) (commitDate CommitDate, err error) {
	if dateStr == "" {
		return CommitDateNow(), fmt.Errorf("date string cannot be empty for new CommitDate")
	}
	t, err := dconfig.ParseDate(dateStr)
	if err != nil {
		return CommitDateNow(), err
	}
	return CommitDateAt(t), nil
}

// CommitIdent holds the name, email, and date for a single author or committer. Use [CommitDateAt]
// for an explicit date or [CommitDateNow] to capture the current time when the commit is written to storage.
type CommitIdent struct {
	Name  string
	Email string
	Date  CommitDate
}

// CommitMeta holds the author and committer identities, commit message, and optional cryptographic
// signature for a commit. Commit process control flags such as whether to amend or allow empty commits are held separately.
type CommitMeta struct {
	Author      CommitIdent
	Committer   CommitIdent
	Description string
	Signature string
}

// resolveDates fixes the author and committer dates in place before serialization. The committer date is
// resolved first, using its explicit value or calling [CommitterDate] if none was set. That resolved time
// is then used to freeze the author date when the author has no explicit date, so both dates share the
// same timestamp when both were created with [CommitDateNow].
func (cm *CommitMeta) resolveDates() {
	committerTime := cm.Committer.Date.resolve()
	cm.Committer.Date = cm.Committer.Date.FreezeAt(committerTime)
	cm.Author.Date = cm.Author.Date.FreezeAt(committerTime)
}

// TimestampMillis returns the committer date as milliseconds since Unix epoch.
func (cm *CommitMeta) TimestampMillis() uint64 {
	return uint64(cm.Committer.Date.resolve().UnixMilli())
}

// UserTimestampMillis returns the author date as milliseconds since Unix epoch.
func (cm *CommitMeta) UserTimestampMillis() int64 {
	return cm.Author.Date.resolve().UnixMilli()
}

// NewCommitMeta returns a [CommitMeta] using |name|, |email|, and |desc| as both the author and committer
// identity. Both dates use [CommitDateNow] and are captured when the commit is written to storage.
func NewCommitMeta(name, email, desc string) (*CommitMeta, error) {
	identity := CommitIdent{Name: name, Email: email, Date: CommitDateNow()}
	return NewCommitMetaWithAuthorAndCommitter(identity, identity, desc)
}

// NewCommitMetaWithAuthorDate returns a [CommitMeta] using |name| and |email| as both the author and committer
// identity, with |authorDate| as the explicit author date. The committer date uses [CommitDateNow] and is
// captured when the commit is written to storage.
func NewCommitMetaWithAuthorDate(name, email, desc string, authorDate time.Time) (*CommitMeta, error) {
	author := CommitIdent{Name: name, Email: email, Date: CommitDateAt(authorDate)}
	committer := CommitIdent{Name: name, Email: email, Date: CommitDateNow()}
	return NewCommitMetaWithAuthorAndCommitter(author, committer, desc)
}

// NewCommitMetaWithAuthorAndCommitter returns a [CommitMeta] with distinct |author| and |committer| identities.
// When either date uses [CommitDateNow], both are resolved to the same timestamp when the commit is written
// to storage.
func NewCommitMetaWithAuthorAndCommitter(author CommitIdent, committer CommitIdent, description string) (*CommitMeta, error) {
	if author.Name == "" {
		return nil, ErrNameNotConfigured.New("author")
	}
	if author.Email == "" {
		return nil, ErrEmailNotConfigured.New("author")
	}
	if description == "" {
		return nil, ErrEmptyCommitMessage
	}
	if committer.Name == "" {
		return nil, ErrNameNotConfigured.New("committer")
	}
	if committer.Email == "" {
		return nil, ErrEmailNotConfigured.New("committer")
	}

	// Dates are left unresolved so that both Author and Committer share the same CommitterDate
	// snapshot when resolveDates is called at serialization time.
	return &CommitMeta{
		Author:      author,
		Committer:   committer,
		Description: description,
	}, nil
}

// FormatTS returns the author date as a [time.RubyDate] string. The timestamp is rounded to the nearest
// second to match MySQL timestamp precision, producing output consistent with the standard Git log format.
func (cm *CommitMeta) FormatTS() string {
	return cm.Author.Date.Time().In(CommitLoc).Round(time.Second).Format(time.RubyDate)
}

// String returns a human-readable summary of the commit metadata.
func (cm *CommitMeta) String() string {
	return fmt.Sprintf("name: %s, email: %s, timestamp: %s, description: %s", cm.Author.Name, cm.Author.Email, cm.FormatTS(), cm.Description)
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
	author := CommitIdent{Name: g.name, Email: g.email, Date: CommitDateAt(g.timestamp)}
	committer := CommitIdent{Name: g.name, Email: g.email, Date: CommitDateNow()}
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
		meta.Author.Name,
		meta.Author.Email,
		meta.Author.Date.Time().String(),
		headHash,
		stagedHash,
	)
}

// SignaturePayloadV2 generates the signature payload including both author and committer information.
// Committer fields are appended after the author fields so V1 is a strict prefix, enabling forward compatibility.
func SignaturePayloadV2(dbName string, meta *CommitMeta, headHash, stagedHash string) string {
	return fmt.Sprintf("%s\nCommitterName: %s\nCommitterEmail: %s\nCommitterDate: %s",
		signaturePayloadV1(dbName, meta, headHash, stagedHash),
		meta.Committer.Name,
		meta.Committer.Email,
		meta.Committer.Date.Time().String())
}
