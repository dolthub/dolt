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
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
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

// CommitterDate is the function used to get the committer time when creating commits.
var CommitterDate = time.Now
var CommitLoc = time.Local

var AuthorDate = time.Now
var CustomAuthorDate bool
var AuthorLoc = time.Local

var CommitterName string
var CommitterEmail string

// CommitMeta contains all the metadata that is associated with a commit within a data repo.
type CommitMeta struct {
	Name           string
	Email          string
	Description    string
	Signature      string
	Timestamp      *uint64
	UserTimestamp  int64
	CommitterName  string
	CommitterEmail string
}

// NewCommitMeta creates a CommitMeta instance from a name, email, and description and uses the current time for the
// timestamp
func NewCommitMeta(name, email, desc string) (*CommitMeta, error) {
	return NewCommitMetaWithUserTimestamp(name, email, desc, AuthorDate())
}

func init() {
	committerDate := os.Getenv(dconfig.EnvDoltCommitterDate)
	if committerDate != "" {
		committerDate, err := dconfig.ParseDate(committerDate)
		if err != nil {
			logrus.Warnf("Unable to parse value for %s: %s. System time will be used instead.",
				dconfig.EnvDoltCommitterDate, err.Error())
		} else {
			CommitterDate = func() time.Time {
				return committerDate
			}
		}
	}

	authorDate := os.Getenv(dconfig.EnvDoltAuthorDate)
	if authorDate != "" {
		authorDate, err := dconfig.ParseDate(authorDate)
		if err != nil {
			logrus.Warnf("Unable to parse value for %s: %s. System time will be used instead.",
				dconfig.EnvDoltAuthorDate, err.Error())
		} else {
			AuthorDate = func() time.Time {
				return authorDate
			}
			CustomAuthorDate = true
		}
	}

	CommitterName = os.Getenv(dconfig.EnvDoltCommitterName)
	CommitterEmail = os.Getenv(dconfig.EnvDoltCommitterEmail)
}

// NewCommitMetaWithUserTimestamp creates a user metadata
func NewCommitMetaWithUserTimestamp(name, email, desc string, userTimestamp time.Time) (*CommitMeta, error) {
	return NewCommitMetaWithAuthorCommitter(name, email, desc, userTimestamp, "", "", nil)
}

// NewCommitMetaWithAuthorCommitter creates commit metadata with separate author and committer information
// If committer info is empty, defaults to author info.
func NewCommitMetaWithAuthorCommitter(authorName, authorEmail, description string, authorTimestamp time.Time, committerName, committerEmail string, committerTimestamp *time.Time) (*CommitMeta, error) {
	authorName = strings.TrimSpace(authorName)
	authorEmail = strings.TrimSpace(authorEmail)
	description = strings.TrimSpace(description)

	committerName = strings.TrimSpace(committerName)
	committerEmail = strings.TrimSpace(committerEmail)

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
		committerName = authorName
	}
	if committerEmail == "" {
		committerEmail = authorEmail
	}

	var committerDateMillis *uint64

	// explicit timestamp overwrite (i.e. when using --date) blocking [datas.CommitterDate()] when creating a new commit
	if committerTimestamp != nil {
		temp := uint64(committerTimestamp.UnixMilli())
		committerDateMillis = &temp
	}

	authorDateMillis := authorTimestamp.UnixMilli()

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

	name := string(authorName.(types.String))
	email := string(authorEmail.(types.String))
	timestamp := uint64(committerTimestamp.(types.Uint))
	return &CommitMeta{
		Name:           name,
		Email:          email,
		Description:    string(authorDescription.(types.String)),
		Signature:      string(signature.(types.String)),
		Timestamp:      &timestamp,
		UserTimestamp:  int64(authorTimestamp.(types.Int)),
		CommitterName:  name,
		CommitterEmail: email,
	}, nil
}

func (cm *CommitMeta) toNomsStruct(nbf *types.NomsBinFormat) (types.Struct, error) {
	metadata := types.StructData{
		commitMetaNameKey:      types.String(cm.Name),
		commitMetaEmailKey:     types.String(cm.Email),
		commitMetaDescKey:      types.String(cm.Description),
		commitMetaTimestampKey: types.Uint(*cm.Timestamp),
		commitMetaVersionKey:   types.String(commitMetaVersion),
		commitMetaUserTSKey:    types.Int(cm.UserTimestamp),
		commitMetaSignature:    types.String(cm.Signature),
	}

	return types.NewStruct(nbf, commitMetaStName, metadata)
}

// Time returns the time at which the commit was authored
// This does not preserve timezone information, and returns the time in the system's local timezone
func (cm *CommitMeta) Time() time.Time {
	return time.UnixMilli(cm.UserTimestamp)
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

// FormatCommitterTS returns the committer timestamp in the standard Git log format.
func (cm *CommitMeta) FormatCommitterTS() string {
	return cm.CommitterTime().In(CommitLoc).Round(time.Second).Format(time.RubyDate)
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
	return NewCommitMetaWithUserTimestamp(g.name, g.email, g.message, g.timestamp)
}

func (*simpleCommitMetaGenerator) IsGoodCommit(*Commit) bool {
	return true
}

func MakeCommitMetaGenerator(name, email string, timestamp time.Time) CommitMetaGenerator {
	return &simpleCommitMetaGenerator{name: name, email: email, timestamp: timestamp, message: defaultInitialCommitMessage, alreadyGenerated: false}
}
