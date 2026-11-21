// Copyright 2026 Dolthub, Inc.
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

import "strings"

// QualifiedDbName is a database name, optionally qualified with a revision: `mydb`, `mydb/branch` or `mydb@branch`.
// Fields retain the caller's case; use Key or DbKey for map lookups.
type QualifiedDbName struct {
	Base     string
	Revision string
}

// ParseQualifiedDbName splits |name| on the first of [DbRevisionDelimiter] or [DbRevisionDelimiterAlias].
func ParseQualifiedDbName(name string) QualifiedDbName {
	base, rev := SplitRevisionDbName(name)
	return QualifiedDbName{Base: base, Revision: rev}
}

// NewQualifiedDbName returns the name for |base| at |revision|. An empty revision means the unqualified name.
func NewQualifiedDbName(base, revision string) QualifiedDbName {
	return QualifiedDbName{Base: base, Revision: revision}
}

func (n QualifiedDbName) IsQualified() bool {
	return n.Revision != ""
}

// String renders the name with the canonical [DbRevisionDelimiter].
func (n QualifiedDbName) String() string {
	if n.Revision == "" {
		return n.Base
	}
	return n.Base + DbRevisionDelimiter + n.Revision
}

// WithRevision returns this name's base qualified with |revision|.
func (n QualifiedDbName) WithRevision(revision string) QualifiedDbName {
	return QualifiedDbName{Base: n.Base, Revision: revision}
}

func (n QualifiedDbName) DbKey() DbKey {
	return DbKey(strings.ToLower(n.Base))
}

func (n QualifiedDbName) Key() QualifiedDbKey {
	return QualifiedDbKey{Db: n.DbKey(), Revision: NewRevisionKey(n.Revision)}
}

// DbKey is the case-normalized base name of a database. Database names are case-insensitive, so this is the only
// form that should be used as a map key.
type DbKey string

// NewDbKey returns the key for the base database of |name|, which may be revision-qualified.
func NewDbKey(name string) DbKey {
	return ParseQualifiedDbName(name).DbKey()
}

// RevisionKey is the case-normalized form of a revision (branch, tag or commit) for use as a map key. Branch and tag
// names resolve case-insensitively.
type RevisionKey string

func NewRevisionKey(revision string) RevisionKey {
	return RevisionKey(strings.ToLower(revision))
}

// QualifiedDbKey is the case-normalized form of a QualifiedDbName.
type QualifiedDbKey struct {
	Db       DbKey
	Revision RevisionKey
}

// NewQualifiedDbKey returns the key for |name|, which may be revision-qualified.
func NewQualifiedDbKey(name string) QualifiedDbKey {
	return ParseQualifiedDbName(name).Key()
}

// String renders the key as a lower-cased name with the canonical [DbRevisionDelimiter].
func (k QualifiedDbKey) String() string {
	return QualifiedDbName{Base: string(k.Db), Revision: string(k.Revision)}.String()
}
