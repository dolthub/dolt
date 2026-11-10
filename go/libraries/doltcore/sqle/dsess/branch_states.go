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

import (
	"strings"
)

// branchStates holds the branch states a session has loaded in its current transaction. A branch state's working
// set, head and write session belong to that transaction, so the session replaces its branchStates when a transaction
// starts or rolls back, rather than emptying it. A branch state is current only while it's in the session's
// branchStates.
type branchStates struct {
	// byDb maps each database's lowercase base name to its branch states, keyed by lowercase head.
	byDb map[string]map[string]*branchState
}

func newBranchStates() *branchStates {
	return &branchStates{byDb: make(map[string]map[string]*branchState)}
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

	lowerHead := strings.ToLower(head)
	if _, ok := dbState.headCache[lowerHead]; !ok {
		dbState.headCache[lowerHead] = newSessionCache()
	}
	return bs
}

func (s *branchStates) put(bs *branchState) {
	db := strings.ToLower(bs.dbState.dbName)
	heads, ok := s.byDb[db]
	if !ok {
		heads = make(map[string]*branchState)
		s.byDb[db] = heads
	}
	heads[strings.ToLower(bs.head)] = bs
}

func (s *branchStates) get(db, head string) (*branchState, bool) {
	bs, ok := s.byDb[strings.ToLower(db)][strings.ToLower(head)]
	return bs, ok
}

// contains reports whether |bs| is the branch state for its branch here.
func (s *branchStates) contains(bs *branchState) bool {
	cur, ok := s.get(bs.dbState.dbName, bs.head)
	return ok && cur == bs
}

func (s *branchStates) remove(db, head string) (*branchState, bool) {
	heads := s.byDb[strings.ToLower(db)]
	bs, ok := heads[strings.ToLower(head)]
	delete(heads, strings.ToLower(head))
	return bs, ok
}

// removeDb removes and returns the branch states of database |db|, keyed by lowercase head.
func (s *branchStates) removeDb(db string) map[string]*branchState {
	heads := s.byDb[strings.ToLower(db)]
	delete(s.byDb, strings.ToLower(db))
	return heads
}

// forDb returns the branch states of database |db|.
func (s *branchStates) forDb(db string) []*branchState {
	heads := s.byDb[strings.ToLower(db)]
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
