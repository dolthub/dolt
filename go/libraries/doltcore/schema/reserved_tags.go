// Copyright 2020 Dolthub, Inc.
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

package schema

import "math"

// ** WARNING - DO NOT CHANGE **
//
// consistency in these values
// is critical for compatibility
//
// ** WARNING - DO NOT CHANGE **

const (
	// SystemTableReservedMin defines the lower bound of the tag space reserved for system tables
	SystemTableReservedMin uint64 = ReservedTagMin << 1
)

// Tags for dolt_docs table
// for info on unaligned constant: https://github.com/dolthub/dolt/pull/663
const (
	// DocNameTag is the tag of the name column in the docs table
	DocNameTag = iota + SystemTableReservedMin + uint64(5)
	// DocTextTag is the tag of the text column in the docs table
	DocTextTag
)

// Tags for dolt_history_ table
const (
	HistoryCommitterTag = iota + SystemTableReservedMin + uint64(1000)
	HistoryCommitHashTag
	HistoryCommitDateTag
)

// Tags for dolt_diff_ table
const (
	DiffCommitTag = iota + SystemTableReservedMin + uint64(2000)
	DiffCommitDateTag
	DiffTypeTag
)

// Tags for dolt_query_catalog table
// for info on unaligned constant: https://github.com/dolthub/dolt/pull/663
const (
	// QueryCatalogIdTag is the tag of the id column in the query catalog table
	QueryCatalogIdTag = iota + SystemTableReservedMin + uint64(3005)
	// QueryCatalogOrderTag is the tag of the column containing the sort order in the query catalog table
	QueryCatalogOrderTag
	// QueryCatalogNameTag is the tag of the column containing the name of the query in the query catalog table
	QueryCatalogNameTag
	// QueryCatalogQueryTag is the tag of the column containing the query in the query catalog table
	QueryCatalogQueryTag
	// QueryCatalogDescriptionTag is the tag of the column containing the query description in the query catalog table
	QueryCatalogDescriptionTag
)

// Tags for dolt_schemas table
// for info on unaligned constant: https://github.com/dolthub/dolt/pull/663
const (
	// Old tag numbers for reference
	//DoltSchemasTypeTag = iota + SystemTableReservedMin + uint64(4003)
	//DoltSchemasNameTag
	//DoltSchemasFragmentTag

	DoltSchemasIdTag = iota + SystemTableReservedMin + uint64(4007)
	DoltSchemasTypeTag
	DoltSchemasNameTag
	DoltSchemasFragmentTag
	DoltSchemasExtraTag
	DoltSchemasSqlModeTag
)

// Tags for hidden columns in keyless rows
const (
	KeylessRowIdTag = iota + SystemTableReservedMin + uint64(5000)
	KeylessRowCardinalityTag
)

// Tags for the dolt_procedures table
const (
	DoltProceduresNameTag = iota + SystemTableReservedMin + uint64(6000)
	DoltProceduresCreateStmtTag
	DoltProceduresCreatedAtTag
	DoltProceduresModifiedAtTag
	DoltProceduresSqlModeTag
)

const (
	DoltConstraintViolationsTypeTag = 0
	DoltConstraintViolationsInfoTag = math.MaxUint64
)

// Tags for the dolt_conflicts_table_name table
const (
	DoltConflictsOurDiffTypeTag = iota + SystemTableReservedMin + uint64(7000)
	DoltConflictsTheirDiffTypeTag
	DoltConflictsBaseCardinalityTag
	DoltConflictsOurCardinalityTag
	DoltConflictsTheirCardinalityTag
)

const (
	DoltIgnorePatternTag = iota + SystemTableReservedMin + uint64(8000)
	DoltIgnoreIgnoredTag
)

// Tags for dolt_ci_workflows table
const (
	// WorkflowsNameTag is the tag of the name column in the workflows table
	WorkflowsNameTag = iota + SystemTableReservedMin + uint64(9000)

	// WorkflowsCreatedAtTag is the tag of the created_at column in the workflows table
	WorkflowsCreatedAtTag

	// WorkflowsUpdatedAtTag is the tag of the updated_at column in the workflows table
	WorkflowsUpdatedAtTag

	// WorkflowEventsIdTag is the tag of the id column in the workflow events table
	WorkflowEventsIdTag

	// WorkflowEventsWorkflowNameFkTag is the tag of the workflow name fk column in the workflow events table
	WorkflowEventsWorkflowNameFkTag

	// WorkflowEventsEventTypeTag is the tag of the events typ column in the workflow events table
	WorkflowEventsEventTypeTag

	// WorkflowEventTriggersIdTag is the tag of the id column of the workflow event triggers table
	WorkflowEventTriggersIdTag

	// WorkflowEventTriggerWorkflowEventIdFkTag is the tag of the workflow events id fk column in the workflow event triggers table
	WorkflowEventTriggerWorkflowEventIdFkTag

	// WorkflowEventTriggerEventTriggerTypeTag is the tag of the event trigger type column on the workflow event triggers table
	WorkflowEventTriggerEventTriggerTypeTag

	// WorkflowEventTriggerBranchesIdTag is the tag of the id columnof the workflow event trigger branches table
	WorkflowEventTriggerBranchesIdTag

	// WorkflowEventTriggerBranchesWorkflowEventTriggerIdFkTag is the tag of the workflow event trigger id foreign key column tag on the workflow event trigger branches table
	WorkflowEventTriggerBranchesWorkflowEventTriggerIdFkTag

	// WorkflowEventTriggerBranchesBranchTag is the tag of the branch column on the workflow event trigger branches table
	WorkflowEventTriggerBranchesBranchTag

	// WorkflowEventTriggerActivitiesIdTag is the tag of the id column on the workflow event trigger activities table
	WorkflowEventTriggerActivitiesIdTag

	// WorkflowEventTriggerActivitiesWorkflowEventTriggerIdFkTag is the name of the tag on the workflow event trigger id foreign key column on the workflow event trigger activities table
	WorkflowEventTriggerActivitiesWorkflowEventTriggerIdFkTag

	// WorkflowEventTriggerActivitiesActivityTag is the name of the tag on the activity column on the workflow event trigger activities table.
	WorkflowEventTriggerActivitiesActivityTag

	// WorkflowJobsIdTag is the name of the tag on the id column on the workflow jobs table
	WorkflowJobsIdTag

	// WorkflowJobsNameTag is the name of the tag on the name column on the workflow jobs table
	WorkflowJobsNameTag

	// WorkflowJobsWorkflowNameFkTag is the name of the tag on the workflow name foreign key column on the
	WorkflowJobsWorkflowNameFkTag

	// WorkflowJobsCreatedAtTag is the name of the tag on the created at column on the workflow jobs table
	WorkflowJobsCreatedAtTag

	// WorkflowJobsUpdatedAtTag is the name of the tag on the updated at column on the workflow jobs table
	WorkflowJobsUpdatedAtTag

	// WorkflowStepsIdTag is the name of the tag on the id column on the workflow steps table
	WorkflowStepsIdTag

	// WorkflowStepsNameTag is the name of the tag on the name tag on the workflow steps table
	WorkflowStepsNameTag

	// WorkflowStepsWorkflowJobIdFkTag is the name of the tag on the workflow job id foreign key column on the workflow steps table
	WorkflowStepsWorkflowJobIdFkTag

	// WorkflowStepsStepOrderTag is the name of the tag on the step order column on the workflow steps table
	WorkflowStepsStepOrderTag

	// WorkflowStepsStepTypeTag is the name of the tag on the step type column on the workflow steps table
	WorkflowStepsStepTypeTag

	// WorkflowStepsCreatedAtTag is the name of the tag on the created at column on the workflow steps table
	WorkflowStepsCreatedAtTag

	// WorkflowStepsUpdatedAtTag is the name of the tag on the updated at column on the workflow steps table
	WorkflowStepsUpdatedAtTag
)
