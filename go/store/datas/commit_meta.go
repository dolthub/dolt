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
	"strings"
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

	commitMetaStName  = "metadata"
	commitMetaVersion = "1.0"
)

var ErrNameNotConfigured = errors.New("Aborting commit due to empty committer name. Is your config set?")
var ErrEmailNotConfigured = errors.New("Aborting commit due to empty committer email. Is your config set?")
var ErrEmptyCommitMessage = errors.New("Aborting commit due to empty commit message.")

var CommitNowFunc = time.Now
var CommitLoc = time.Local

// CommitMeta contains all the metadata that is associated with a commit within a data repo.
type CommitMeta struct {
	Name          string
	Email         string
	Timestamp     uint64
	Description   string
	UserTimestamp int64
}

// NewCommitMeta creates a CommitMeta instance from a name, email, and description and uses the current time for the
// timestamp
func NewCommitMeta(name, email, desc string) (*CommitMeta, error) {
	return NewCommitMetaWithUserTS(name, email, desc, CommitNowFunc())
}

// NewCommitMetaWithUserTS creates a user metadata
func NewCommitMetaWithUserTS(name, email, desc string, userTS time.Time) (*CommitMeta, error) {
	n := strings.TrimSpace(name)
	e := strings.TrimSpace(email)
	d := strings.TrimSpace(desc)

	if n == "" {
		return nil, ErrNameNotConfigured
	}

	if e == "" {
		return nil, ErrEmailNotConfigured
	}

	if d == "" {
		return nil, ErrEmptyCommitMessage
	}

	ms := uint64(CommitNowFunc().UnixMilli())
	userMS := userTS.UnixMilli()

	return &CommitMeta{n, e, ms, d, userMS}, nil
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

	return &CommitMeta{
		string(n.(types.String)),
		string(e.(types.String)),
		uint64(ts.(types.Uint)),
		string(d.(types.String)),
		int64(userTS.(types.Int)),
	}, nil
}

func (cm *CommitMeta) toNomsStruct(nbf *types.NomsBinFormat) (types.Struct, error) {
	metadata := types.StructData{
		commitMetaNameKey:      types.String(cm.Name),
		commitMetaEmailKey:     types.String(cm.Email),
		commitMetaDescKey:      types.String(cm.Description),
		commitMetaTimestampKey: types.Uint(cm.Timestamp),
		commitMetaVersionKey:   types.String(commitMetaVersion),
		commitMetaUserTSKey:    types.Int(cm.UserTimestamp),
	}

	return types.NewStruct(nbf, commitMetaStName, metadata)
}

// Time returns the time at which the commit occurred
func (cm *CommitMeta) Time() time.Time {
	return time.UnixMilli(cm.UserTimestamp)
}

// FormatTS takes the internal timestamp and turns it into a human readable string in the time.RubyDate format
// which looks like: "Mon Jan 02 15:04:05 -0700 2006"
func (cm *CommitMeta) FormatTS() string {
	return cm.Time().In(CommitLoc).Format(time.RubyDate)
}

// String returns the human readable string representation of the commit data
func (cm *CommitMeta) String() string {
	return fmt.Sprintf("name: %s, email: %s, timestamp: %s, description: %s", cm.Name, cm.Email, cm.FormatTS(), cm.Description)
}
