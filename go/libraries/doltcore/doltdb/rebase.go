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

package doltdb

// TODO: It would be nice to keep these types in the rebase package, but that causes an import cycle :-(
//       See how we can clean this structure up better

type RebasePlan struct {
	Members []RebasePlanMember
}

type RebasePlanMember struct {
	RebaseOrder uint   // TODO: If we change the schema to be a DECIMAL(6,2), uint won't work anymore...
	Action      string // TODO: how to easily sync up this action with the enum types? –Add some helper functions to this type
	CommitHash  string
	CommitMsg   string
}
