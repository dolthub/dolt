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

package datas

import (
	"fmt"
	"strings"
	"time"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	tagMetaNameKey      = "name"
	tagMetaEmailKey     = "email"
	tagMetaDescKey      = "desc"
	tagMetaTimestampKey = "timestamp"
	tagMetaUserTSKey    = "user_timestamp"
	tagMetaVersionKey   = "metaversion"

	tagMetaStName  = "metadata"
	tagMetaVersion = "1.0"
)

var TagNowFunc = CommitNowFunc
var TagLoc = CommitLoc

// TagMeta contains all the metadata that is associated with a tag within a data repo.
type TagMeta struct {
	Name          string
	Email         string
	Timestamp     uint64
	Description   string
	UserTimestamp int64
}

// NewTagMetaWithUserTS returns TagMeta that can be used to create a tag.
// It uses the current time as the user timestamp.
func NewTagMeta(name, email, desc string) *TagMeta {
	return NewTagMetaWithUserTS(name, email, desc, TagNowFunc())
}

// NewTagMetaWithUserTS returns TagMeta that can be used to create a tag.
func NewTagMetaWithUserTS(name, email, desc string, userTS time.Time) *TagMeta {
	n := strings.TrimSpace(name)
	e := strings.TrimSpace(email)
	d := strings.TrimSpace(desc)

	ms := uint64(TagNowFunc().UnixMilli())
	userMS := userTS.UnixMilli()

	return &TagMeta{n, e, ms, d, userMS}
}

func tagMetaFromNomsSt(st types.Struct) (*TagMeta, error) {
	e, err := getRequiredFromSt(st, tagMetaEmailKey)

	if err != nil {
		return nil, err
	}

	n, err := getRequiredFromSt(st, tagMetaNameKey)

	if err != nil {
		return nil, err
	}

	d, err := getRequiredFromSt(st, tagMetaDescKey)

	if err != nil {
		return nil, err
	}

	ts, err := getRequiredFromSt(st, tagMetaTimestampKey)

	if err != nil {
		return nil, err
	}

	userTS, ok, err := st.MaybeGet(tagMetaUserTSKey)

	if err != nil {
		return nil, err
	} else if !ok {
		userTS = types.Int(int64(uint64(ts.(types.Uint))))
	}

	return &TagMeta{
		string(n.(types.String)),
		string(e.(types.String)),
		uint64(ts.(types.Uint)),
		string(d.(types.String)),
		int64(userTS.(types.Int)),
	}, nil
}

func (tm *TagMeta) toNomsStruct(nbf *types.NomsBinFormat) (types.Struct, error) {
	metadata := types.StructData{
		tagMetaNameKey:      types.String(tm.Name),
		tagMetaEmailKey:     types.String(tm.Email),
		tagMetaDescKey:      types.String(tm.Description),
		tagMetaTimestampKey: types.Uint(tm.Timestamp),
		tagMetaVersionKey:   types.String(tagMetaVersion),
		commitMetaUserTSKey: types.Int(tm.UserTimestamp),
	}

	return types.NewStruct(nbf, tagMetaStName, metadata)
}

// Time returns the time at which the tag occurred
func (tm *TagMeta) Time() time.Time {
	return time.UnixMilli(int64(tm.Timestamp))
}

// FormatTS takes the internal timestamp and turns it into a human readable string in the time.RubyDate format
// which looks like: "Mon Jan 02 15:04:05 -0700 2006"
func (tm *TagMeta) FormatTS() string {
	return tm.Time().In(TagLoc).Format(time.RubyDate)
}

// String returns the human readable string representation of the tag data
func (tm *TagMeta) String() string {
	return fmt.Sprintf("name: %s, email: %s, timestamp: %s, description: %s", tm.Name, tm.Email, tm.FormatTS(), tm.Description)
}
