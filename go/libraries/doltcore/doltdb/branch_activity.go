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
	"sync"
	"time"
)

const (
	READ = iota
	WRITE
)

// BranchActivityData represents activity data for a single branch
type BranchActivityData struct {
	Branch          string
	LastRead        *time.Time
	LastWrite       *time.Time
	SystemStartTime time.Time
}

type branchActivityEvent struct {
	branch    string
	timestamp time.Time
	eventType int
}

var (
	branchActivityMutex sync.RWMutex
	branchReadTimes     map[string]time.Time
	branchWriteTimes    map[string]time.Time
	systemStartTime     time.Time
	activityChan        chan branchActivityEvent
)

func init() {
	systemStartTime = time.Now()
	branchReadTimes = make(map[string]time.Time)
	branchWriteTimes = make(map[string]time.Time)
	activityChan = make(chan branchActivityEvent, 64) // lifetime in buffer will be very short.

	// Start background goroutine to process events
	go func() {
		ctx := context.Background()
		for {
			select {
			case event := <-activityChan:
				branchActivityMutex.Lock()
				if event.eventType == READ {
					if existing, exists := branchReadTimes[event.branch]; !exists || event.timestamp.After(existing) {
						branchReadTimes[event.branch] = event.timestamp
					}
				} else if event.eventType == WRITE {
					if existing, exists := branchWriteTimes[event.branch]; !exists || event.timestamp.After(existing) {
						branchWriteTimes[event.branch] = event.timestamp
					}
				}
				branchActivityMutex.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// BranchActivityReadEvent records when a branch is read/accessed
func BranchActivityReadEvent(branch string) {
	select {
	case activityChan <- branchActivityEvent{
		branch:    branch,
		timestamp: time.Now(),
		eventType: READ,
	}:
	default:
		// Channel is full, drop the event
	}
}

// GetBranchActivity returns activity data for all tracked branches
func GetBranchActivity() []BranchActivityData {
	branchActivityMutex.RLock()
	defer branchActivityMutex.RUnlock()

	// Collect all unique branch names
	branches := make(map[string]bool)
	for branch := range branchReadTimes {
		branches[branch] = true
	}
	for branch := range branchWriteTimes {
		branches[branch] = true
	}

	result := make([]BranchActivityData, 0, len(branches))
	for branch := range branches {
		data := BranchActivityData{
			Branch:          branch,
			SystemStartTime: systemStartTime,
		}

		if readTime, exists := branchReadTimes[branch]; exists {
			data.LastRead = &readTime
		}

		if writeTime, exists := branchWriteTimes[branch]; exists {
			data.LastWrite = &writeTime
		}

		result = append(result, data)
	}

	// Sort by primary key (branch name)
	sort.Slice(result, func(i, j int) bool {
		return result[i].Branch < result[j].Branch
	})

	return result
}
