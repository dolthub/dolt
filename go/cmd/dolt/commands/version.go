// Copyright 2019 Liquidata, Inc.
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

package commands

import (
	"context"

	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
)

type VersionCmd struct {
	VersionStr string
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd VersionCmd) Name() string {
	return "version"
}

// Description returns a description of the command
func (cmd VersionCmd) Description() string {
	return "Displays the current Dolt cli version."
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd VersionCmd) RequiresRepo() bool {
	return false
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd VersionCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	return nil
}

// Version displays the version of the running dolt client
// Exec executes the command
func (cmd VersionCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	cli.Println("dolt version", cmd.VersionStr)

	return 0
}
