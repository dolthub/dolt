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

package schema

import "math"

// ** WARNING - DO NOT CHANGE **
//
// consistency in these values
// is critical for compatibility
//
// ** WARNING - DO NOT CHANGE **

const (
	// SystemTableReservedMin defines the lower bound of the tag space reserved for system tables
	SystemTableReservedMin uint64 = ReservedTagMin << 1
)

// Tags for dolt_docs table
// for info on unaligned constant: https://github.com/dolthub/dolt/pull/663
const (
	// DocNameTag is the tag of the name column in the docs table
	DocNameTag = iota + SystemTableReservedMin + uint64(5)
	// DocTextTag is the tag of the text column in the docs table
	DocTextTag
)

// Tags for dolt_history_ table
const (
	HistoryCommitterTag = iota + SystemTableReservedMin + uint64(1000)
	HistoryCommitHashTag
	HistoryCommitDateTag
)

// Tags for dolt_diff_ table
const (
	DiffCommitTag = iota + SystemTableReservedMin + uint64(2000)
	DiffCommitDateTag
	DiffTypeTag
)

// Tags for dolt_query_catalog table
// for info on unaligned constant: https://github.com/dolthub/dolt/pull/663
const (
	// QueryCatalogIdTag is the tag of the id column in the query catalog table
	QueryCatalogIdTag = iota + SystemTableReservedMin + uint64(3005)
	// QueryCatalogOrderTag is the tag of the column containing the sort order in the query catalog table
	QueryCatalogOrderTag
	// QueryCatalogNameTag is the tag of the column containing the name of the query in the query catalog table
	QueryCatalogNameTag
	// QueryCatalogQueryTag is the tag of the column containing the query in the query catalog table
	QueryCatalogQueryTag
	// QueryCatalogDescriptionTag is the tag of the column containing the query description in the query catalog table
	QueryCatalogDescriptionTag
)

// Tags for dolt_schemas table
// for info on unaligned constant: https://github.com/dolthub/dolt/pull/663
const (
	// Old tag numbers for reference
	//DoltSchemasTypeTag = iota + SystemTableReservedMin + uint64(4003)
	//DoltSchemasNameTag
	//DoltSchemasFragmentTag

	DoltSchemasIdTag = iota + SystemTableReservedMin + uint64(4007)
	DoltSchemasTypeTag
	DoltSchemasNameTag
	DoltSchemasFragmentTag
	DoltSchemasExtraTag
)

// Tags for hidden columns in keyless rows
const (
	KeylessRowIdTag = iota + SystemTableReservedMin + uint64(5000)
	KeylessRowCardinalityTag
)

// Tags for the dolt_procedures table
const (
	DoltProceduresNameTag = iota + SystemTableReservedMin + uint64(6000)
	DoltProceduresCreateStmtTag
	DoltProceduresCreatedAtTag
	DoltProceduresModifiedAtTag
)

const (
	DoltConstraintViolationsTypeTag = 0
	DoltConstraintViolationsInfoTag = math.MaxUint64
)
