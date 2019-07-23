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
	"os/exec"
	"path/filepath"

	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/utils"
)

// Install configures this git repository for use with git-dolt; specifically, it sets up the
// smudge filter that automatically clones dolt repos when git-dolt pointer files are checked out.
func Install() error {
	if _, err := exec.LookPath("git-dolt-smudge"); err != nil {
		return fmt.Errorf("can't find git-dolt-smudge in PATH")
	}

	gitPath, err := utils.FindGitConfigUnderRoot()
	if err != nil {
		return err
	}

	gitParentPath := filepath.Dir(gitPath)
	gitAttributesPath := filepath.Join(gitParentPath, ".gitattributes")
	if err := utils.AppendToFile(gitAttributesPath, "*.git-dolt filter=git-dolt"); err != nil {
		return err
	}

	gitConfigPath := filepath.Join(gitPath, "config")
	if err := utils.AppendToFile(gitConfigPath, "[filter \"git-dolt\"]\n\tsmudge = git-dolt-smudge"); err != nil {
		return err
	}

	fmt.Println("Installed git-dolt smudge filter. When git-dolt pointer files are checked out in this git repository, the corresponding Dolt repositories will be automatically cloned.")
	fmt.Println("\nYou should git commit the changes to .gitattributes.")
	return nil
}
