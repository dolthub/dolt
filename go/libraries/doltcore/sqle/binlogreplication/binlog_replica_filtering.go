// Copyright 2023 Dolthub, Inc.
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

package binlogreplication

import (
	"fmt"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
)

// filterConfiguration defines the binlog filtering rules applied on the replica.
type filterConfiguration struct {
	// doTables holds a map of database name to map of table names, indicating tables that SHOULD be replicated.
	doTables map[string]map[string]struct{}
	// ignoreTables holds a map of database name to map of table names, indicating tables that should NOT be replicated.
	ignoreTables map[string]map[string]struct{}
	// mu guards against concurrent access to the filter configuration data.
	mu *sync.Mutex
}

// newFilterConfiguration creates a new filterConfiguration instance and initializes members.
func newFilterConfiguration() *filterConfiguration {
	return &filterConfiguration{
		doTables:     make(map[string]map[string]struct{}),
		ignoreTables: make(map[string]map[string]struct{}),
		mu:           &sync.Mutex{},
	}
}

// setDoTables sets the tables that are allowed to replicate and returns an error if any problems were
// encountered, such as unqualified tables being specified in |urts|. If any DoTables were previously configured,
// they are cleared out before the new tables are set as the value of DoTables.
func (fc *filterConfiguration) setDoTables(urts []sql.UnresolvedTable) error {
	err := verifyAllTablesAreQualified(urts)
	if err != nil {
		return err
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()

	// Setting new replication filters clears out any existing filters
	fc.doTables = make(map[string]map[string]struct{})

	for _, urt := range urts {
		if fc.doTables[urt.Database()] == nil {
			fc.doTables[urt.Database()] = make(map[string]struct{})
		}
		tableMap := fc.doTables[urt.Database()]
		tableMap[urt.Name()] = struct{}{}
	}
	return nil
}

// setIgnoreTables sets the tables that are NOT allowed to replicate and returns an error if any problems were
// encountered, such as unqualified tables being specified in |urts|. If any IgnoreTables were previously configured,
// they are cleared out before the new tables are set as the value of IgnoreTables.
func (fc *filterConfiguration) setIgnoreTables(urts []sql.UnresolvedTable) error {
	err := verifyAllTablesAreQualified(urts)
	if err != nil {
		return err
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()

	// Setting new replication filters clears out any existing filters
	fc.ignoreTables = make(map[string]map[string]struct{})

	for _, urt := range urts {
		if fc.ignoreTables[urt.Database()] == nil {
			fc.ignoreTables[urt.Database()] = make(map[string]struct{})
		}
		tableMap := fc.ignoreTables[urt.Database()]
		tableMap[urt.Name()] = struct{}{}
	}
	return nil
}

// isTableFilteredOut returns true if the table identified by |tableMap| has been filtered out on this replica and
// should not have any updates applied from binlog messages.
func (fc *filterConfiguration) isTableFilteredOut(tableMap *mysql.TableMap) bool {
	if fc == nil {
		return false
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()

	// If any filter doTable options are specified, then a table MUST be listed in the set
	// for it to be replicated. doTables options are processed BEFORE ignoreTables options.
	// If a table appears in both doTable and ignoreTables, it is ignored.
	// https://dev.mysql.com/doc/refman/8.0/en/replication-rules-table-options.html
	if len(fc.doTables) > 0 {
		if doTables, ok := fc.doTables[tableMap.Database]; ok {
			if _, ok := doTables[tableMap.Name]; !ok {
				logger.Tracef("skipping table %s.%s (not in doTables) ", tableMap.Database, tableMap.Name)
				return true
			}
		}
	}

	if len(fc.ignoreTables) > 0 {
		if ignoredTables, ok := fc.ignoreTables[tableMap.Database]; ok {
			if _, ok := ignoredTables[tableMap.Name]; ok {
				// If this table is being ignored, don't process any further
				logger.Tracef("skipping table %s.%s (in ignoreTables)", tableMap.Database, tableMap.Name)
				return true
			}
		}
	}

	return false
}

// getDoTables returns a slice of qualified table names that are configured to be replicated.
func (fc *filterConfiguration) getDoTables() []string {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return convertFilterMapToStringSlice(fc.doTables)
}

// getIgnoreTables returns a slice of qualified table names that are configured to be filtered out of replication.
func (fc *filterConfiguration) getIgnoreTables() []string {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return convertFilterMapToStringSlice(fc.ignoreTables)
}

// convertFilterMapToStringSlice converts the specified |filterMap| into a string slice, by iterating over every
// key in the top level map, which stores a database name, and for each of those keys, iterating over every key
// in the inner map, which stores a table name. Each table name is qualified with the matching database name and the
// results are returned as a slice of qualified table names.
func convertFilterMapToStringSlice(filterMap map[string]map[string]struct{}) []string {
	if filterMap == nil {
		return nil
	}

	tableNames := make([]string, 0, len(filterMap))
	for dbName, tableMap := range filterMap {
		for tableName, _ := range tableMap {
			tableNames = append(tableNames, fmt.Sprintf("%s.%s", dbName, tableName))
		}
	}
	return tableNames
}
