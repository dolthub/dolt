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

// This is a starting point for storing common messages. Doing this correctly would probably mean using language files
// but that is overkill for the moment.
const (
	// Single variable - the name of the command. `dolt <command>` is how the commandString is formatted in calls to the Exec method
	// for dolt commands.
	RemoteUnsupportedMsg = "%s can not currently be used when there is a local server running. Please stop your dolt sql-server or connect using `dolt sql` instead."
)
