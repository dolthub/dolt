// Copyright 2019 Liquidata, Inc.
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

package doltdb

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/utils/funcitr"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
)

const (
	// DoltNamespace is the name prefix of dolt system tables. We reserve all tables that begin with dolt_ for system use.
	DoltNamespace = "dolt_"

	// SystemTableReservedMin defines the lower bound of the tag space reserved for system tables
	SystemTableReservedMin uint64 = schema.ReservedTagMin << 1
)

var ErrSystemTableCannotBeModified = errors.New("system tables cannot be dropped or altered")

// HasDoltPrefix returns a boolean whether or not the provided string is prefixed with the DoltNamespace. Users should
// not be able to create tables in this reserved namespace.
func HasDoltPrefix(s string) bool {
	return strings.HasPrefix(s, DoltNamespace)
}

// IsReadOnlySystemTable returns whether the table name given is a system table that should not be included in command line
// output (e.g. dolt status) by default.
func IsReadOnlySystemTable(name string) bool {
	return HasDoltPrefix(name) && !set.NewStrSet(writeableSystemTables).Contains(name)
}

// GetNonSystemTableNames gets non-system table names
func GetNonSystemTableNames(ctx context.Context, root *RootValue) ([]string, error) {
	tn, err := root.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	tn = funcitr.FilterStrings(tn, func(n string) bool {
		return !HasDoltPrefix(n)
	})
	sort.Strings(tn)
	return tn, nil
}

// GetSystemTableNames gets system table names
func GetSystemTableNames(ctx context.Context, root *RootValue) ([]string, error) {
	p, err := GetPersistedSystemTables(ctx, root)
	if err != nil {
		return nil, err
	}

	g, err := GetGeneratedSystemTables(ctx, root)
	if err != nil {

	}

	s := append(p, g...)
	sort.Strings(s)

	return s, nil
}

// GetPersistedSystemTables returns table names of all persisted system tables.
func GetPersistedSystemTables(ctx context.Context, root *RootValue) ([]string, error) {
	tn, err := root.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	sort.Strings(tn)
	return funcitr.FilterStrings(tn, HasDoltPrefix), nil
}

// GetGeneratedSystemTables returns table names of all generated system tables.
func GetGeneratedSystemTables(ctx context.Context, root *RootValue) ([]string, error) {
	s := set.NewStrSet(generatedSystemTables)

	tn, err := root.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	for _, pre := range generatedSystemTablePrefixes {
		s.Add(funcitr.MapStrings(tn, func(s string) string { return pre + s })...)
	}
	return s.AsSlice(), nil
}

// GetAllTableNames returns table names for all persisted and generated tables.
func GetAllTableNames(ctx context.Context, root *RootValue) ([]string, error) {
	n, err := GetNonSystemTableNames(ctx, root)
	if err != nil {
		return nil, err
	}
	s, err := GetSystemTableNames(ctx, root)
	if err != nil {
		return nil, err
	}
	return append(n, s...), nil
}

// The set of reserved dolt_ tables that should be considered part of user space, like any other user-created table,
// for the purposes of the dolt command line. These tables cannot be created or altered explicitly, but can be updated
// like normal SQL tables.
var writeableSystemTables = []string{
	DoltQueryCatalogTableName,
	SchemasTableName,
}

var persistedSystemTables = []string{
	DocTableName,
	DoltQueryCatalogTableName,
	SchemasTableName,
}

var generatedSystemTables = []string{
	BranchesTableName,
	LogTableName,
}

var generatedSystemTablePrefixes = []string{
	DoltDiffTablePrefix,
	DoltHistoryTablePrefix,
}

const (
	DocTableName      = "dolt_docs"
	LicensePk         = "LICENSE.md"
	ReadmePk          = "README.md"
	DocPkColumnName   = "doc_name"
	DocTextColumnName = "doc_text"
)
const (
	// Tags for dolt_docs table
	DocNameTag = iota + SystemTableReservedMin + uint64(5)
	DocTextTag
)

const (
	// DoltQueryCatalogTableName is the name of the query catalog table
	DoltQueryCatalogTableName = "dolt_query_catalog"

	// QueryCatalogIdCol is the name of the primary key column of the query catalog table
	QueryCatalogIdCol = "id"

	// QueryCatalogOrderCol is the column containing the order of the queries in the catalog
	QueryCatalogOrderCol = "display_order"

	// QueryCatalogNameCol is the name of the column containing the name of a query in the catalog
	QueryCatalogNameCol = "name"

	// QueryCatalogQueryCol is the name of the column containing the query of a catalog entry
	QueryCatalogQueryCol = "query"

	// QueryCatalogDescriptionCol is the name of the column containing the description of a query in the catalog
	QueryCatalogDescriptionCol = "description"
)
const (
	// Tags for dolt_query_catalog table
	QueryCatalogIdTag = iota + SystemTableReservedMin + uint64(3005)
	QueryCatalogOrderTag
	QueryCatalogNameTag
	QueryCatalogQueryTag
	QueryCatalogDescriptionTag
)

const (
	// SchemasTableName is the name of the dolt schema fragment table
	SchemasTableName = "dolt_schemas"

	// Currently: `view`.
	SchemasTablesTypeCol = "type"

	// // The name of the database entity.
	SchemasTablesNameCol = "name"
	// The schema fragment associated with the database entity.
	// For example, the SELECT statement for a CREATE VIEW.
	SchemasTablesFragmentCol = "fragment"
)
const (
	// Tags for dolt_schemas table
	DoltSchemasTypeTag = iota + SystemTableReservedMin + uint64(4003)
	DoltSchemasNameTag
	DoltSchemasFragmentTag
)

const (
	DoltHistoryTablePrefix = "dolt_history_"
)
const (
	// Tags for dolt_history_ table
	HistoryCommitterTag = iota + SystemTableReservedMin + uint64(1000)
	HistoryCommitHashTag
	HistoryCommitDateTag
)

const (
	DoltDiffTablePrefix = "dolt_diff_"
)
const (
	// Tags for dolt_diff_ table
	DiffCommitTag = iota + SystemTableReservedMin + uint64(2000)
)

const (
	// BranchesTableName is the system table name
	BranchesTableName = "dolt_branches"
)

const (
	// LogTableName is the system table name
	LogTableName = "dolt_log"
)
