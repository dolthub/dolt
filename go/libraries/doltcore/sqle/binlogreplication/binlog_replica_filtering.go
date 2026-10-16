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
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	gmsbinlogreplication "github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/vitess/go/mysql"
)

const (
	literalToken tablePatternTokenKind = iota
	singleWildcardToken
	multiWildcardToken
)

// filterConfiguration defines row-event filtering rules applied on the replica.
// QueryEvents, including DDL and statement-format DML, are outside this filter's current scope.
type filterConfiguration struct {
	// mu guards filter replacement while allowing concurrent row-event reads.
	mu sync.RWMutex
	// doTables holds a map of database name to map of table names, indicating tables that SHOULD be replicated.
	doTables map[string]map[string]struct{}
	// ignoreTables holds a map of database name to map of table names, indicating tables that should NOT be replicated.
	ignoreTables map[string]map[string]struct{}
	// wildDoTables holds ordered wildcard rules that SHOULD be replicated.
	wildDoTables []compiledTablePattern
	// wildIgnoreTables holds ordered wildcard rules that should NOT be replicated.
	wildIgnoreTables []compiledTablePattern
	// caseInsensitive matches the replica's lower_case_table_names setting.
	caseInsensitive bool
}

// compiledTablePattern keeps the configured pattern alongside its matcher tokens.
type compiledTablePattern struct {
	tokens []tablePatternToken
	raw    string
}

// tablePatternToken is one byte-oriented operation in a compiled table pattern.
type tablePatternToken struct {
	literal byte
	kind    tablePatternTokenKind
}

// tablePatternTokenKind identifies how a compiled table-pattern token matches input.
type tablePatternTokenKind byte

// newFilterConfiguration creates a new filterConfiguration instance and initializes members.
func newFilterConfiguration() *filterConfiguration {
	return &filterConfiguration{
		doTables:     make(map[string]map[string]struct{}),
		ignoreTables: make(map[string]map[string]struct{}),
	}
}

// clear removes every configured replication filter while preserving references held by the applier.
func (fc *filterConfiguration) clear() {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.doTables = make(map[string]map[string]struct{})
	fc.ignoreTables = make(map[string]map[string]struct{})
	fc.wildDoTables = nil
	fc.wildIgnoreTables = nil
	fc.caseInsensitive = false
}

// setOptions validates a complete command before atomically replacing the filter types it specifies.
func (fc *filterConfiguration) setOptions(options []gmsbinlogreplication.ReplicationOption, caseInsensitive bool) error {
	var updates struct {
		doTables, ignoreTables         map[string]map[string]struct{}
		wildDoTables, wildIgnoreTables []compiledTablePattern
		setDo, setIgnore               bool
		setWildDo, setWildIgnore       bool
	}

	for _, option := range options {
		var err error
		switch strings.ToUpper(option.Name) {
		case "REPLICATE_DO_TABLE":
			value, valueErr := getOptionValue[[]sql.UnresolvedTable](option, "a list of tables")
			if valueErr != nil {
				return valueErr
			}
			updates.doTables, err = buildExactTableFilter(value, caseInsensitive)
			updates.setDo = true
		case "REPLICATE_IGNORE_TABLE":
			value, valueErr := getOptionValue[[]sql.UnresolvedTable](option, "a list of tables")
			if valueErr != nil {
				return valueErr
			}
			updates.ignoreTables, err = buildExactTableFilter(value, caseInsensitive)
			updates.setIgnore = true
		case "REPLICATE_WILD_DO_TABLE":
			value, valueErr := getOptionValue[[]string](option, "a list of strings")
			if valueErr != nil {
				return valueErr
			}
			updates.wildDoTables, err = compileTablePatterns(value, caseInsensitive)
			updates.setWildDo = true
		case "REPLICATE_WILD_IGNORE_TABLE":
			value, valueErr := getOptionValue[[]string](option, "a list of strings")
			if valueErr != nil {
				return valueErr
			}
			updates.wildIgnoreTables, err = compileTablePatterns(value, caseInsensitive)
			updates.setWildIgnore = true
		default:
			return fmt.Errorf("unsupported replication filter option: %s", option.Name)
		}
		if err != nil {
			return err
		}
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.caseInsensitive = caseInsensitive
	if updates.setDo {
		fc.doTables = updates.doTables
	}
	if updates.setIgnore {
		fc.ignoreTables = updates.ignoreTables
	}
	if updates.setWildDo {
		fc.wildDoTables = updates.wildDoTables
	}
	if updates.setWildIgnore {
		fc.wildIgnoreTables = updates.wildIgnoreTables
	}
	return nil
}

// buildExactTableFilter validates and builds an exact-table filter map.
func buildExactTableFilter(urts []sql.UnresolvedTable, caseInsensitive bool) (map[string]map[string]struct{}, error) {
	err := verifyAllTablesAreQualified(urts)
	if err != nil {
		return nil, err
	}

	filterMap := make(map[string]map[string]struct{})
	for _, urt := range urts {
		table := normalizeFilterName(urt.Name(), caseInsensitive)
		db := normalizeFilterName(urt.Database().Name(), caseInsensitive)
		if filterMap[db] == nil {
			filterMap[db] = make(map[string]struct{})
		}
		tableMap := filterMap[db]
		tableMap[table] = struct{}{}
	}
	return filterMap, nil
}

// compileTablePatterns validates and compiles ordered wildcard filters for hot-path matching.
func compileTablePatterns(patterns []string, caseInsensitive bool) ([]compiledTablePattern, error) {
	if err := gmsbinlogreplication.ValidateWildcardTablePatterns(patterns); err != nil {
		return nil, err
	}
	filters := make([]compiledTablePattern, len(patterns))
	for i, pattern := range patterns {
		filters[i] = compiledTablePattern{raw: pattern, tokens: compileWildcardPattern(normalizeFilterName(pattern, caseInsensitive))}
	}
	return filters, nil
}

// normalizeFilterName applies the replica's identifier case policy for matching.
func normalizeFilterName(value string, caseInsensitive bool) string {
	if caseInsensitive {
		return strings.ToLower(value)
	}
	return value
}

// compileWildcardPattern converts replication wildcards into byte-oriented matcher tokens.
func compileWildcardPattern(pattern string) []tablePatternToken {
	tokens := make([]tablePatternToken, 0, len(pattern))
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '\\':
			if i+1 < len(pattern) {
				i++
				tokens = append(tokens, tablePatternToken{kind: literalToken, literal: pattern[i]})
			} else {
				tokens = append(tokens, tablePatternToken{kind: literalToken, literal: '\\'})
			}
		case '_':
			tokens = append(tokens, tablePatternToken{kind: singleWildcardToken})
		case '%':
			if len(tokens) == 0 || tokens[len(tokens)-1].kind != multiWildcardToken {
				tokens = append(tokens, tablePatternToken{kind: multiWildcardToken})
			}
		default:
			tokens = append(tokens, tablePatternToken{kind: literalToken, literal: pattern[i]})
		}
	}
	return tokens
}

// isTableFilteredOut returns true if the table identified by |tableMap| has been filtered out on this replica and
// should not have any updates applied from binlog messages.
func (fc *filterConfiguration) isTableFilteredOut(ctx *sql.Context, tableMap *mysql.TableMap) bool {
	if fc == nil {
		return false
	}

	fc.mu.RLock()
	defer fc.mu.RUnlock()

	table := normalizeFilterName(tableMap.Name, fc.caseInsensitive)
	db := normalizeFilterName(tableMap.Database, fc.caseInsensitive)
	qualifiedTable := db + "." + table

	// MySQL checks exact-do, exact-ignore, wildcard-do, then wildcard-ignore rules, returning
	// immediately on a match. If no rule matches, the presence of any do rule excludes the table.
	// https://dev.mysql.com/doc/refman/8.0/en/replication-rules-table-options.html
	if exactTableFilterMatches(fc.doTables, db, table) {
		return false
	}

	if exactTableFilterMatches(fc.ignoreTables, db, table) {
		ctx.GetLogger().Tracef("skipping table %s.%s (in ignoreTables)", tableMap.Database, tableMap.Name)
		return true
	}

	if tablePatternsMatch(fc.wildDoTables, qualifiedTable) {
		return false
	}

	if tablePatternsMatch(fc.wildIgnoreTables, qualifiedTable) {
		ctx.GetLogger().Tracef("skipping table %s.%s (in wildIgnoreTables)", tableMap.Database, tableMap.Name)
		return true
	}

	if len(fc.doTables) > 0 || len(fc.wildDoTables) > 0 {
		ctx.GetLogger().Tracef("skipping table %s.%s (not in doTables or wildDoTables)", tableMap.Database, tableMap.Name)
		return true
	}

	return false
}

// exactTableFilterMatches reports whether an exact table filter contains the table.
func exactTableFilterMatches(filters map[string]map[string]struct{}, database, table string) bool {
	if tables, ok := filters[database]; ok {
		if _, ok := tables[table]; ok {
			return true
		}
	}
	return false
}

// tablePatternsMatch reports whether any compiled pattern matches a qualified table name.
func tablePatternsMatch(patterns []compiledTablePattern, qualifiedTable string) bool {
	for _, pattern := range patterns {
		if wildcardPatternMatches(pattern.tokens, qualifiedTable) {
			return true
		}
	}
	return false
}

// wildcardPatternMatches applies a compiled MySQL replication wildcard to bytes in a qualified table name.
func wildcardPatternMatches(tokens []tablePatternToken, value string) bool {
	tokenIndex, valueIndex := 0, 0
	lastManyIndex, lastManyValueIndex := -1, 0
	for valueIndex < len(value) {
		if tokenIndex < len(tokens) && tokens[tokenIndex].kind == literalToken && tokens[tokenIndex].literal == value[valueIndex] {
			tokenIndex++
			valueIndex++
		} else if tokenIndex < len(tokens) && tokens[tokenIndex].kind == singleWildcardToken {
			tokenIndex++
			valueIndex++
		} else if tokenIndex < len(tokens) && tokens[tokenIndex].kind == multiWildcardToken {
			lastManyIndex = tokenIndex
			tokenIndex++
			lastManyValueIndex = valueIndex
		} else if lastManyIndex >= 0 {
			tokenIndex = lastManyIndex + 1
			lastManyValueIndex++
			valueIndex = lastManyValueIndex
		} else {
			return false
		}
	}
	for tokenIndex < len(tokens) && tokens[tokenIndex].kind == multiWildcardToken {
		tokenIndex++
	}
	return tokenIndex == len(tokens)
}

// tableFilters returns one consistent copy of the configured filter values for status reporting.
func (fc *filterConfiguration) tableFilters() (doTables, ignoreTables, wildDoTables, wildIgnoreTables []string) {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return convertFilterMapToStringSlice(fc.doTables),
		convertFilterMapToStringSlice(fc.ignoreTables),
		rawTablePatterns(fc.wildDoTables),
		rawTablePatterns(fc.wildIgnoreTables)
}

// rawTablePatterns copies the configured strings from compiled table patterns.
func rawTablePatterns(compiled []compiledTablePattern) []string {
	patterns := make([]string, len(compiled))
	for i, pattern := range compiled {
		patterns[i] = pattern.raw
	}
	return patterns
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
