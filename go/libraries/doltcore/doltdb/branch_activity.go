// Copyright 2025 Dolthub, Inc.
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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

const (
	READ = iota
	WRITE
)

type statsSessionContextKeyType struct{}

// StatsSessionContextKey is used to mark sql sessions which are related to stats processing. We don't want to count
// reads/writes from these sessions in branch activity tracking.
var StatsSessionContextKey = statsSessionContextKeyType{}

// BranchActivityProvider interface allows sessions to provide branch activity tracking
type BranchActivityProvider interface {
	GetBranchActivityTracker() *BranchActivityTracker
}

type eventSessionContextKeyType struct{}

// EventSessionContextKey is used to mark sql sessions which are related to backround event. We don't want to count
// reads/writes from these sessions in branch activity tracking.
var EventSessionContextKey = eventSessionContextKeyType{}

// BranchActivityData represents activity data for a single branch
type BranchActivityData struct {
	Branch          string
	LastRead        *time.Time
	LastWrite       *time.Time
	SystemStartTime time.Time
}

type branchActivityEvent struct {
	database  string
	branch    string
	timestamp time.Time
	eventType int
}

type branchActivityKey struct {
	database string
	branch   string
}

// BranchActivityTracker tracks branch activity for a single SQL engine instance
type BranchActivityTracker struct {
	mu              sync.RWMutex
	readTimes       map[branchActivityKey]time.Time
	writeTimes      map[branchActivityKey]time.Time
	systemStartTime time.Time
	activityChan    chan branchActivityEvent
}

// NewBranchActivityTracker creates a new branch activity tracker instance
func NewBranchActivityTracker(ctx context.Context) *BranchActivityTracker {
	tracker := &BranchActivityTracker{
		readTimes:       make(map[branchActivityKey]time.Time),
		writeTimes:      make(map[branchActivityKey]time.Time),
		systemStartTime: time.Now(),
		activityChan:    make(chan branchActivityEvent, 64),
	}

	// Start background processor, we ignore the cancel function as the tracker doesn't have a lifecycle.
	ctx, _ = context.WithCancel(ctx)
	go tracker.processEvents(ctx)

	return tracker
}

// processEvents processes activity events in the background
func (t *BranchActivityTracker) processEvents(ctx context.Context) {
	for {
		select {
		case event := <-t.activityChan:
			key := branchActivityKey{database: event.database, branch: event.branch}

			t.mu.Lock()
			if event.eventType == READ {
				if existing, exists := t.readTimes[key]; !exists || event.timestamp.After(existing) {
					t.readTimes[key] = event.timestamp
				}
			} else if event.eventType == WRITE {
				if existing, exists := t.writeTimes[key]; !exists || event.timestamp.After(existing) {
					t.writeTimes[key] = event.timestamp
				}
			}
			t.mu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

// RecordReadEvent records when a branch is read/accessed
func (t *BranchActivityTracker) RecordReadEvent(ctx context.Context, database, branch string) {
	if ignoreEvent(ctx, branch) {
		return
	}

	select {
	case t.activityChan <- branchActivityEvent{
		database:  database,
		branch:    branch,
		timestamp: time.Now(),
		eventType: READ,
	}:
	default:
		// Channel is full, drop the event
	}
}

// RecordWriteEvent records when a branch is written/updated
func (t *BranchActivityTracker) RecordWriteEvent(ctx context.Context, database, branch string) {
	if ignoreEvent(ctx, branch) {
		return
	}

	select {
	case t.activityChan <- branchActivityEvent{
		database:  database,
		branch:    branch,
		timestamp: time.Now(),
		eventType: WRITE,
	}:
	default:
		// Lots of traffic. drop the event
	}
}

// GetBranchActivity returns activity data for all current branches in the specified database
func (t *BranchActivityTracker) GetBranchActivity(ctx *sql.Context, ddb *DoltDB) ([]BranchActivityData, error) {
	database := ctx.GetCurrentDatabase()
	parts := strings.SplitN(database, "/", 2)
	database = parts[0]

	t.mu.RLock()
	defer t.mu.RUnlock()

	branchRefs, err := ddb.GetBranches(ctx)
	if err != nil {
		return nil, err
	}

	branches := make(map[string]bool)
	for _, branchRef := range branchRefs {
		branches[branchRef.GetPath()] = true
	}

	result := make([]BranchActivityData, 0, len(branches))
	for branch := range branches {
		data := BranchActivityData{
			Branch:          branch,
			SystemStartTime: t.systemStartTime,
		}

		key := branchActivityKey{database: database, branch: branch}

		if readTime, exists := t.readTimes[key]; exists {
			data.LastRead = &readTime
		}

		if writeTime, exists := t.writeTimes[key]; exists {
			data.LastWrite = &writeTime
		}

		result = append(result, data)
	}

	// Sort by primary key (branch name)
	sort.Slice(result, func(i, j int) bool {
		return result[i].Branch < result[j].Branch
	})

	return result, nil
}

// BranchActivityReadEvent records when a branch is read/accessed - this is for backward compatibility
// The database parameter should be extracted by the caller using dsess.SplitRevisionDbName
func BranchActivityReadEvent(ctx *sql.Context, database, branch string) {
	if provider, ok := ctx.Session.(BranchActivityProvider); ok {
		tracker := provider.GetBranchActivityTracker()
		if tracker != nil {
			tracker.RecordReadEvent(ctx, database, branch)
		}
	}
}

// BranchActivityWriteEvent records when a branch is written/updated - this is for backward compatibility
// The database parameter should be extracted by the caller using dsess.SplitRevisionDbName
func BranchActivityWriteEvent(ctx *sql.Context, database, branch string) {
	if provider, ok := ctx.Session.(BranchActivityProvider); ok {
		tracker := provider.GetBranchActivityTracker()
		if tracker != nil {
			tracker.RecordWriteEvent(ctx, database, branch)
		}
	}
}

// ignoreEvent determines whether to ignore the event based on the context and branch name. We ignore events
// from sessions related to stats processing or event scheduler, as well as events on the HEAD branch.
func ignoreEvent(ctx context.Context, branch string) bool {
	if ctx.Value(StatsSessionContextKey) != nil {
		return true
	}
	if ctx.Value(EventSessionContextKey) != nil {
		return true
	}
	if branch == "HEAD" {
		return true
	}

	return false
}

// GetBranchActivity returns activity data for all current branches
func GetBranchActivity(ctx *sql.Context, ddb *DoltDB) ([]BranchActivityData, error) {
	if provider, ok := ctx.Session.(BranchActivityProvider); ok {
		tracker := provider.GetBranchActivityTracker()
		if tracker != nil {
			return tracker.GetBranchActivity(ctx, ddb)
		}
	}
	return nil, nil
}
