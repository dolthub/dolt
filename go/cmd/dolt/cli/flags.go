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

package cli

// Constants for command line flags names. These tend to be used in multiple places, so defining
// them low in the package dependency tree makes sense.
const (
	AbortParam           = "abort"
	AllFlag              = "all"
	AllowEmptyFlag       = "allow-empty"
	AmendFlag            = "amend"
	AuthorParam          = "author"
	BranchParam          = "branch"
	CachedFlag           = "cached"
	CheckoutCreateBranch = "b"
	CommitFlag           = "commit"
	CopyFlag             = "copy"
	DateParam            = "date"
	DecorateFlag         = "decorate"
	DeleteFlag           = "delete"
	DeleteForceFlag      = "D"
	DryRunFlag           = "dry-run"
	ForceFlag            = "force"
	HardResetParam       = "hard"
	HostFlag             = "host"
	ListFlag             = "list"
	MergesFlag           = "merges"
	MessageArg           = "message"
	MinParentsFlag       = "min-parents"
	MoveFlag             = "move"
	NoCommitFlag         = "no-commit"
	NoEditFlag           = "no-edit"
	NoFFParam            = "no-ff"
	NoPrettyFlag         = "no-pretty"
	NoTLSFlag            = "no-tls"
	NotFlag              = "not"
	NumberFlag           = "number"
	OneLineFlag          = "oneline"
	OursFlag             = "ours"
	OutputOnlyFlag       = "output-only"
	ParentsFlag          = "parents"
	PasswordFlag         = "password"
	PortFlag             = "port"
	PruneFlag            = "prune"
	RemoteParam          = "remote"
	SetUpstreamFlag      = "set-upstream"
	ShallowFlag          = "shallow"
	ShowIgnoredFlag      = "ignored"
	SkipEmptyFlag        = "skip-empty"
	SoftResetParam       = "soft"
	SquashParam          = "squash"
	TablesFlag           = "tables"
	TheirsFlag           = "theirs"
	TrackFlag            = "track"
	UpperCaseAllFlag     = "ALL"
	UserFlag             = "user"
)
