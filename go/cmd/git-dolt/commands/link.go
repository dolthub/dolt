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
	"fmt"

	"github.com/liquidata-inc/dolt/go/cmd/git-dolt/config"
	"github.com/liquidata-inc/dolt/go/cmd/git-dolt/doltops"
	"github.com/liquidata-inc/dolt/go/cmd/git-dolt/env"
	"github.com/liquidata-inc/dolt/go/cmd/git-dolt/utils"
)

// Link creates a git-dolt pointer file linking the given dolt remote
// to the current git repository.
func Link(remote string) error {
	if err := doltops.Clone(remote); err != nil {
		return err
	}

	dirname := utils.LastSegment(remote)
	revision, err := utils.CurrentRevision(dirname)
	if err != nil {
		return err
	}

	c := config.GitDoltConfig{Version: env.Version, Remote: remote, Revision: revision}
	if err := config.Write(dirname, c.String()); err != nil {
		return err
	}

	if err := utils.AppendToFile(".gitignore", dirname); err != nil {
		return err
	}

	fmt.Printf("\nDolt repository linked!\n\n")
	fmt.Printf("* Repository cloned to %s at revision %s\n", dirname, revision)
	fmt.Printf("* Pointer file created at %s.git-dolt\n", dirname)
	fmt.Printf("* %s added to .gitignore\n\n", dirname)
	fmt.Println("You should git commit these results.")
	return nil
}
