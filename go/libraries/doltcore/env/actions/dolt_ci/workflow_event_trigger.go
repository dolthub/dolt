// Copyright 2024 Dolthub, Inc.
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

package dolt_ci

import (
	"errors"
	"strings"
)

var ErrUnknownWorkflowEventTriggerType = errors.New("unknown workflow event trigger type")
var ErrUnknownWorkflowEventTriggerActivityType = errors.New("unknown workflow event trigger activity type")

type WorkflowEventTriggerType int

const (
	WorkflowEventTriggerTypeUnspecified WorkflowEventTriggerType = iota
	WorkflowEventTriggerTypeBranches
	WorkflowEventTriggerTypeActivityOpened
	WorkflowEventTriggerTypeActivityClosed
	WorkflowEventTriggerTypeActivityReopened
	WorkflowEventTriggerTypeActivitySynchronized
)

type WorkflowEventTriggerId string

type WorkflowEventTrigger struct {
	Id                *WorkflowEventTriggerId  `db:"id"`
	WorkflowEventIdFK *WorkflowEventId         `db:"workflow_event_id_fk"`
	EventTriggerType  WorkflowEventTriggerType `db:"event_trigger_type"`
}

// ToWorkflowEventTriggerType is used to change an in to a valid WorkflowEventTriggerType
func ToWorkflowEventTriggerType(t int) (WorkflowEventTriggerType, error) {
	switch t {
	case int(WorkflowEventTriggerTypeBranches):
		return WorkflowEventTriggerTypeBranches, nil
	case int(WorkflowEventTriggerTypeActivityOpened):
		return WorkflowEventTriggerTypeActivityOpened, nil
	case int(WorkflowEventTriggerTypeActivityClosed):
		return WorkflowEventTriggerTypeActivityClosed, nil
	case int(WorkflowEventTriggerTypeActivityReopened):
		return WorkflowEventTriggerTypeActivityReopened, nil
	case int(WorkflowEventTriggerTypeActivitySynchronized):
		return WorkflowEventTriggerTypeActivitySynchronized, nil
	default:
		return WorkflowEventTriggerTypeUnspecified, ErrUnknownWorkflowEventTriggerType
	}
}

// WorkflowEventTriggerActivityTypeToString is used to change a valid WorkflowEventTriggerType to a string
func WorkflowEventTriggerActivityTypeToString(t WorkflowEventTriggerType) (string, error) {
	switch t {
	case WorkflowEventTriggerTypeActivityOpened:
		return "opened", nil
	case WorkflowEventTriggerTypeActivityClosed:
		return "closed", nil
	case WorkflowEventTriggerTypeActivityReopened:
		return "reopened", nil
	case WorkflowEventTriggerTypeActivitySynchronized:
		return "synchronized", nil
	default:
		return "", ErrUnknownWorkflowEventTriggerType
	}
}

// ToWorkflowEventTriggerActivityType is used to change a string to a valid WorkflowEventTriggerType
func ToWorkflowEventTriggerActivityType(str string) (WorkflowEventTriggerType, error) {
	switch strings.ToLower(str) {
	case "opened":
		return WorkflowEventTriggerTypeActivityOpened, nil
	case "closed":
		return WorkflowEventTriggerTypeActivityClosed, nil
	case "reopened":
		return WorkflowEventTriggerTypeActivityReopened, nil
	case "synchronized":
		return WorkflowEventTriggerTypeActivitySynchronized, nil
	default:
		return WorkflowEventTriggerTypeUnspecified, ErrUnknownWorkflowEventTriggerType
	}
}
