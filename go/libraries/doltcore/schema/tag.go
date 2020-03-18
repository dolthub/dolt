// Copyright 2020 Liquidata, Inc.
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

package schema

import (
	"math"
	"math/rand"
	"time"
)

const (
	// TODO: increase ReservedTagMin to 1 << 63 once numeric marshalling is fixed
	// ReservedTagMin is the start of a range of tags which the user should not be able to use in their schemas.
	ReservedTagMin uint64 = 1 << 50

	//
	SystemTableReservedMin uint64 = 1 << 51

	// InvalidTag is used as an invalid tag
	InvalidTag uint64 = math.MaxUint64
)

const (
	// Tags for dolt_docs table
	DocNameTag = iota + SystemTableReservedMin
	DocTextTag

	// Tags for dolt_history_ table
	HistoryCommitterTag
	HistoryCommitHashTag
	HistoryCommitDateTag

	// Tags for dolt_diff_ table
	DiffCommitTag

	// Tags for dolt_query_catalog table
	QueryCatalogIdTag
	QueryCatalogOrderTag
	QueryCatalogNameTag
	QueryCatalogQueryTag
	QueryCatalogDescriptionTag

	// Tags for dolt_schemas table
	DoltSchemasTypeTag
	DoltSchemasNameTag
	DoltSchemasFragmentTag
)

var randGen = rand.New(rand.NewSource(time.Now().UnixNano()))

// AutoGenerateTag generates a random tag that doesn't exist in the provided SuperSchema
func AutoGenerateTag(ss *SuperSchema) uint64 {
	var maxTagVal uint64 = 128 * 128

	for maxTagVal/2 < uint64(ss.Size()) {
		if maxTagVal == ReservedTagMin-1 {
			panic("There is no way anyone should ever have this many columns.  You are a bad person if you hit this panic.")
		} else if maxTagVal*128 < maxTagVal {
			maxTagVal = ReservedTagMin - 1
		} else {
			maxTagVal = maxTagVal * 128
		}
	}

	var randTag uint64
	for {
		randTag = uint64(randGen.Int63n(int64(maxTagVal)))

		if _, found := ss.GetColumn(randTag); !found {
			break
		}
	}

	return randTag
}
