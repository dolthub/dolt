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

var ErrUnknownWorkflowEventType = errors.New("unknown workflow event type")

type WorkflowEventType int

const (
	WorkflowEventTypeUnspecified WorkflowEventType = iota
	WorkflowEventTypePush
	WorkflowEventTypePullRequest
	WorkflowEventTypeWorkflowDispatch
)

type WorkflowEventId string

type WorkflowEvent struct {
	Id             *WorkflowEventId  `db:"id"`
	WorkflowNameFK *WorkflowName     `db:"workflow_name_fk"`
	EventType      WorkflowEventType `db:"event_type"`
}

func toWorkflowEventType(t int) (WorkflowEventType, error) {
	switch t {
	case int(WorkflowEventTypePush):
		return WorkflowEventTypePush, nil
	case int(WorkflowEventTypePullRequest):
		return WorkflowEventTypePullRequest, nil
	case int(WorkflowEventTypeWorkflowDispatch):
		return WorkflowEventTypeWorkflowDispatch, nil
	default:
		return WorkflowEventTypeUnspecified, ErrUnknownWorkflowEventType
	}
}
