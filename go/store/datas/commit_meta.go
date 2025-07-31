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
	// Deprecated keys - maintained for backward compatibility
	commitMetaNameKey      = "name"           // Deprecated: maps to author_name
	commitMetaEmailKey     = "email"          // Deprecated: maps to author_email
	commitMetaTimestampKey = "timestamp"      // Deprecated: maps to committer_date
	commitMetaUserTSKey    = "user_timestamp" // Deprecated: maps to author_date

	// New keys for author/committer separation
	commitMetaAuthorNameKey     = "author_name"
	commitMetaAuthorEmailKey    = "author_email"
	commitMetaAuthorDateKey     = "author_date"
	commitMetaCommitterNameKey  = "committer_name"
	commitMetaCommitterEmailKey = "committer_email"
	commitMetaCommitterDateKey  = "committer_date"

	// Other keys
	commitMetaDescKey    = "desc"
	commitMetaVersionKey = "metaversion"
	commitMetaSignature  = "signature"

	commitMetaStName  = "metadata"
	commitMetaVersion = "2.0" // Bumped for author/committer separation
)

const defaultInitialCommitMessage = "Initialize data repository"

var ErrNameNotConfigured = errors.New("Aborting commit due to empty committer name. Is your config set?")
var ErrEmailNotConfigured = errors.New("Aborting commit due to empty committer email. Is your config set?")
var ErrEmptyCommitMessage = errors.New("Aborting commit due to empty commit message.")

// CommitterDate is the function used to get the committer time when creating commits.
var CommitterDate = time.Now
var CommitLoc = time.Local

var AuthorDate = time.Now
var CustomAuthorDate = func() bool {
	return false
}
var AuthorLoc = time.Local

func init() {
	// Parse committer date once at startup
	if committerDateStr := os.Getenv(dconfig.EnvDoltCommitterDate); committerDateStr != "" {
		if committerDate, err := dconfig.ParseDate(committerDateStr); err != nil {
			logrus.Warnf("Unable to parse value for %s: %s. System time will be used instead.",
				dconfig.EnvDoltCommitterDate, err.Error())
		} else {
			CommitterDate = func() time.Time {
				return committerDate
			}
		}
	}

	// Parse author date once at startup
	if authorDateStr := os.Getenv(dconfig.EnvDoltAuthorDate); authorDateStr != "" {
		if authorDate, err := dconfig.ParseDate(authorDateStr); err != nil {
			logrus.Warnf("Unable to parse value for %s: %s. System time will be used instead.",
				dconfig.EnvDoltAuthorDate, err.Error())
		} else {
			AuthorDate = func() time.Time {
				return authorDate
			}
			CustomAuthorDate = func() bool {
				return true
			}
		}
	}
}

// CommitMeta contains all the metadata that is associated with a commit within a data repo.
type CommitMeta struct {
	// Author information - who originally wrote the code
	AuthorName  string
	AuthorEmail string
	AuthorDate  uint64 // Unix timestamp in milliseconds

	// Committer information - who created the commit
	CommitterName  string
	CommitterEmail string
	CommitterDate  uint64 // Unix timestamp in milliseconds

	// Other metadata
	Description string
	Signature   string

	// Deprecated fields maintained for backward compatibility
	// These will be removed in a future major version
	Name          string // Deprecated: maps to AuthorName for compatibility
	Email         string // Deprecated: maps to AuthorEmail for compatibility
	Timestamp     uint64 // Deprecated: maps to CommitterDate for compatibility
	UserTimestamp int64  // Deprecated: maps to AuthorDate for compatibility
}

// NewCommitMeta creates a CommitMeta instance from a name, email, and description and uses the current time for the
// timestamp. For backward compatibility, the name and email are used for both author and committer.
func NewCommitMeta(name, email, desc string) (*CommitMeta, error) {
	return NewCommitMetaWithUserTS(name, email, desc, AuthorDate())
}

// NewCommitMetaWithUserTS creates a CommitMeta with custom author timestamp.
// For backward compatibility, the name and email are used for both author and committer.
func NewCommitMetaWithUserTS(name, email, desc string, userTS time.Time) (*CommitMeta, error) {
	return NewCommitMetaWithAuthorAndCommitter(
		name, email, userTS,
		name, email, CommitterDate(),
		desc,
	)
}

// NewCommitMetaWithAuthorAndCommitter creates a CommitMeta with separate author and committer information.
func NewCommitMetaWithAuthorAndCommitter(
	authorName, authorEmail string, authorDate time.Time,
	committerName, committerEmail string, committerDate time.Time,
	desc string,
) (*CommitMeta, error) {
	an := strings.TrimSpace(authorName)
	ae := strings.TrimSpace(authorEmail)
	cn := strings.TrimSpace(committerName)
	ce := strings.TrimSpace(committerEmail)
	d := strings.TrimSpace(desc)

	if an == "" || cn == "" {
		return nil, ErrNameNotConfigured
	}

	if ae == "" || ce == "" {
		return nil, ErrEmailNotConfigured
	}

	if d == "" {
		return nil, ErrEmptyCommitMessage
	}

	authorDateMillis := uint64(authorDate.UnixMilli())
	committerDateMillis := uint64(committerDate.UnixMilli())

	return &CommitMeta{
		// New fields
		AuthorName:     an,
		AuthorEmail:    ae,
		AuthorDate:     authorDateMillis,
		CommitterName:  cn,
		CommitterEmail: ce,
		CommitterDate:  committerDateMillis,
		Description:    d,
		Signature:      "",
		// Deprecated fields for backward compatibility
		Name:          an,
		Email:         ae,
		Timestamp:     committerDateMillis,
		UserTimestamp: int64(authorDateMillis),
	}, nil
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
	// Try to read new format first
	authorName, hasAuthorName, err := st.MaybeGet(commitMetaAuthorNameKey)
	if err != nil {
		return nil, err
	}

	if hasAuthorName {
		// New format with separate author/committer
		authorEmail, err := getRequiredFromSt(st, commitMetaAuthorEmailKey)
		if err != nil {
			return nil, err
		}

		authorDate, err := getRequiredFromSt(st, commitMetaAuthorDateKey)
		if err != nil {
			return nil, err
		}

		committerName, err := getRequiredFromSt(st, commitMetaCommitterNameKey)
		if err != nil {
			return nil, err
		}

		committerEmail, err := getRequiredFromSt(st, commitMetaCommitterEmailKey)
		if err != nil {
			return nil, err
		}

		committerDate, err := getRequiredFromSt(st, commitMetaCommitterDateKey)
		if err != nil {
			return nil, err
		}

		desc, err := getRequiredFromSt(st, commitMetaDescKey)
		if err != nil {
			return nil, err
		}

		signature, ok, err := st.MaybeGet(commitMetaSignature)
		if err != nil {
			return nil, err
		} else if !ok {
			signature = types.String("")
		}

		return &CommitMeta{
			// New fields
			AuthorName:     string(authorName.(types.String)),
			AuthorEmail:    string(authorEmail.(types.String)),
			AuthorDate:     uint64(authorDate.(types.Uint)),
			CommitterName:  string(committerName.(types.String)),
			CommitterEmail: string(committerEmail.(types.String)),
			CommitterDate:  uint64(committerDate.(types.Uint)),
			Description:    string(desc.(types.String)),
			Signature:      string(signature.(types.String)),
			// Deprecated fields for compatibility
			Name:          string(authorName.(types.String)),
			Email:         string(authorEmail.(types.String)),
			Timestamp:     uint64(committerDate.(types.Uint)),
			UserTimestamp: int64(authorDate.(types.Uint)),
		}, nil
	}

	// Fall back to old format
	e, err := getRequiredFromSt(st, commitMetaEmailKey)
	if err != nil {
		return nil, err
	}

	n, err := getRequiredFromSt(st, commitMetaNameKey)
	if err != nil {
		return nil, err
	}

	d, err := getRequiredFromSt(st, commitMetaDescKey)
	if err != nil {
		return nil, err
	}

	ts, err := getRequiredFromSt(st, commitMetaTimestampKey)
	if err != nil {
		return nil, err
	}

	userTS, ok, err := st.MaybeGet(commitMetaUserTSKey)
	if err != nil {
		return nil, err
	} else if !ok {
		userTS = types.Int(int64(uint64(ts.(types.Uint))))
	}

	signature, ok, err := st.MaybeGet(commitMetaSignature)
	if err != nil {
		return nil, err
	} else if !ok {
		signature = types.String("")
	}

	// For old format commits, author and committer are the same
	name := string(n.(types.String))
	email := string(e.(types.String))
	timestamp := uint64(ts.(types.Uint))
	userTimestamp := int64(userTS.(types.Int))

	return &CommitMeta{
		// New fields - set author and committer to same values
		AuthorName:     name,
		AuthorEmail:    email,
		AuthorDate:     uint64(userTimestamp),
		CommitterName:  name,
		CommitterEmail: email,
		CommitterDate:  timestamp,
		Description:    string(d.(types.String)),
		Signature:      string(signature.(types.String)),
		// Deprecated fields
		Name:          name,
		Email:         email,
		Timestamp:     timestamp,
		UserTimestamp: userTimestamp,
	}, nil
}

func (cm *CommitMeta) toNomsStruct(nbf *types.NomsBinFormat) (types.Struct, error) {
	// Write both old and new format fields for backward compatibility
	metadata := types.StructData{
		// New format fields
		commitMetaAuthorNameKey:     types.String(cm.AuthorName),
		commitMetaAuthorEmailKey:    types.String(cm.AuthorEmail),
		commitMetaAuthorDateKey:     types.Uint(cm.AuthorDate),
		commitMetaCommitterNameKey:  types.String(cm.CommitterName),
		commitMetaCommitterEmailKey: types.String(cm.CommitterEmail),
		commitMetaCommitterDateKey:  types.Uint(cm.CommitterDate),
		// Old format fields for backward compatibility
		commitMetaNameKey:      types.String(cm.Name),
		commitMetaEmailKey:     types.String(cm.Email),
		commitMetaTimestampKey: types.Uint(cm.Timestamp),
		commitMetaUserTSKey:    types.Int(cm.UserTimestamp),
		// Common fields
		commitMetaDescKey:    types.String(cm.Description),
		commitMetaVersionKey: types.String(commitMetaVersion),
		commitMetaSignature:  types.String(cm.Signature),
	}

	return types.NewStruct(nbf, commitMetaStName, metadata)
}

// Time returns the time at which the commit occurred (author time for backward compatibility)
func (cm *CommitMeta) Time() time.Time {
	return time.UnixMilli(int64(cm.AuthorDate))
}

// AuthorTime returns the time when the changes were originally authored
func (cm *CommitMeta) AuthorTime() time.Time {
	return time.UnixMilli(int64(cm.AuthorDate))
}

// CommitterTime returns the time when the commit was created
func (cm *CommitMeta) CommitterTime() time.Time {
	return time.UnixMilli(int64(cm.CommitterDate))
}

// FormatTS takes the internal timestamp and turns it into a human readable string in the time.RubyDate format
// which looks like: "Mon Jan 02 15:04:05 -0700 2006"
//
// We round this to the nearest second, which is what MySQL timestamp does by default.
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
	return NewCommitMetaWithUserTS(g.name, g.email, g.message, g.timestamp)
}

func (*simpleCommitMetaGenerator) IsGoodCommit(*Commit) bool {
	return true
}

func MakeCommitMetaGenerator(name, email string, timestamp time.Time) CommitMetaGenerator {
	return &simpleCommitMetaGenerator{name: name, email: email, timestamp: timestamp, message: defaultInitialCommitMessage, alreadyGenerated: false}
}
