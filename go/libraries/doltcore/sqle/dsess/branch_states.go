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

package dsess

import "github.com/dolthub/dolt/go/libraries/doltcore/doltdb"

// branchStates holds the branch states a session has loaded in its current transaction. A branch state's working
// set, head and write session belong to that transaction, so the session replaces its branchStates when a transaction
// starts or rolls back, rather than emptying it. A branch state is current only while it's in the session's
// branchStates.
type branchStates struct {
	// byDb maps each database to its branch states.
	byDb map[doltdb.DbKey]map[doltdb.RevisionKey]*branchState
}

func newBranchStates() *branchStates {
	return &branchStates{byDb: make(map[doltdb.DbKey]map[doltdb.RevisionKey]*branchState)}
}

// add creates an empty branch state for |head| of |dbState|'s database, replacing any it already has, and returns it.
// The caller fills it in.
func (s *branchStates) add(dbState *DatabaseSessionState, head string, revisionType RevisionType) *branchState {
	bs := &branchState{
		dbState:      dbState,
		head:         head,
		revisionType: revisionType,
	}
	s.put(bs)

	key := doltdb.NewRevisionKey(head)
	if _, ok := dbState.headCache[key]; !ok {
		dbState.headCache[key] = newSessionCache()
	}
	return bs
}

func (s *branchStates) put(bs *branchState) {
	key := bs.key()
	heads, ok := s.byDb[key.Db]
	if !ok {
		heads = make(map[doltdb.RevisionKey]*branchState)
		s.byDb[key.Db] = heads
	}
	heads[key.Revision] = bs
}

func (s *branchStates) get(name doltdb.QualifiedDbName) (*branchState, bool) {
	key := name.Key()
	bs, ok := s.byDb[key.Db][key.Revision]
	return bs, ok
}

// contains reports whether |bs| is the branch state for its branch here.
func (s *branchStates) contains(bs *branchState) bool {
	cur, ok := s.get(bs.name())
	return ok && cur == bs
}

func (s *branchStates) remove(name doltdb.QualifiedDbName) (*branchState, bool) {
	key := name.Key()
	heads := s.byDb[key.Db]
	bs, ok := heads[key.Revision]
	delete(heads, key.Revision)
	return bs, ok
}

// removeDb removes and returns the branch states of |name|'s database. Its revision is ignored.
func (s *branchStates) removeDb(name doltdb.QualifiedDbName) map[doltdb.RevisionKey]*branchState {
	db := name.DbKey()
	heads := s.byDb[db]
	delete(s.byDb, db)
	return heads
}

// forDb returns the branch states of |name|'s database. Its revision is ignored.
func (s *branchStates) forDb(name doltdb.QualifiedDbName) []*branchState {
	heads := s.byDb[name.DbKey()]
	states := make([]*branchState, 0, len(heads))
	for _, bs := range heads {
		states = append(states, bs)
	}
	return states
}

// all returns every branch state.
func (s *branchStates) all() []*branchState {
	var states []*branchState
	for _, heads := range s.byDb {
		for _, bs := range heads {
			states = append(states, bs)
		}
	}
	return states
}

// checkNoOpenWriters returns an error if any branch state has open table writers.
func (s *branchStates) checkNoOpenWriters() error {
	for _, heads := range s.byDb {
		for _, bs := range heads {
			if err := bs.checkNoOpenWriters(); err != nil {
				return err
			}
		}
	}
	return nil
}
