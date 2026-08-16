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

// Package schemadrift implements the `dolt admin schema-encoding-drift` admin
// commands — check (read-only diagnose) and repair (atomic per-column flip).
//
// The drift this package targets is the upstream Dolt 2.0.7 regression where
// AlterableDoltTable.ModifyColumn (and the rewrite-path schema rebuild) lost a
// column's persisted storage encoding while leaving the on-disk row data
// untouched. A 1.x-written TEXT / BLOB / JSON / GEOMETRY column survives an
// upgrade with `StringAddrEnc(23)` (or sibling) in the persisted schema, but
// after any ALTER on the same table the schema record silently flips to
// `StringAdaptiveEnc(135)` while the row payload is still in the legacy raw
// 20-byte-hash format. Adaptive dispatch then panics with `invalid hash length:
// 19` on TEXT/BLOB and silently misclassifies the persisted format on JSON /
// GEOMETRY.
//
// The schema-side fix (commit `4969194e2` and `09278d859`) prevents NEW ALTERs
// from extending the corruption — but any column that has already been ALTERed
// past 2.0.7 has the wrong tag in its persisted schema. This package is the
// explicit, atomic, audit-logged repair path the operator runs to bring those
// columns back into alignment with their on-disk rows. There is no read-path
// fallback; if you read a corrupted column before running `repair`, dolt
// panics. That is the deliberate postgres-tier contract.
package schemadrift

import (
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
)

// Commands is the parent admin subcommand handler. Registered from
// `admin.go` as `Commands` for the `schema-encoding-drift` slot.
var Commands = cli.NewHiddenSubCommandHandler("schema-encoding-drift",
	"Detect and repair persisted-schema / on-disk-row encoding drift left by the v2.0.7 ALTER-MODIFY regression",
	[]cli.Command{
		CheckCmd{},
		RepairCmd{},
		RecoverRowsCmd{},
		MigrateAdaptiveCmd{},
	},
)
