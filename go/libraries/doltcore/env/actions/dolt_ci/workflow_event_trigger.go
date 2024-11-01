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
)

var ErrUnknownWorkflowEventTriggerType = errors.New("unknown workflow event trigger type")

type WorkflowEventTriggerType int

const (
	WorkflowEventTriggerTypeUnspecified WorkflowEventTriggerType = iota
	WorkflowEventTriggerTypeBranches
	WorkflowEventTriggerTypeActivities
	WorkflowEventTriggerTypeWorkflowDispatch
)

type WorkflowEventTriggerId string

type WorkflowEventTrigger struct {
	Id                *WorkflowEventTriggerId  `db:"id"`
	WorkflowEventIdFK *WorkflowEventId         `db:"workflow_event_id_fk"`
	EventTriggerType  WorkflowEventTriggerType `db:"event_trigger_type"`
}

func toWorkflowEventTriggerType(t int) (WorkflowEventTriggerType, error) {
	switch t {
	case int(WorkflowEventTriggerTypeBranches):
		return WorkflowEventTriggerTypeBranches, nil
	case int(WorkflowEventTriggerTypeActivities):
		return WorkflowEventTriggerTypeActivities, nil
	case int(WorkflowEventTriggerTypeWorkflowDispatch):
		return WorkflowEventTriggerTypeWorkflowDispatch, nil
	default:
		return WorkflowEventTriggerTypeUnspecified, ErrUnknownWorkflowEventTriggerType
	}
}
