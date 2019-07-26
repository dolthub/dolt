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

package main

import (
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/liquidata-inc/dolt/go/cmd/git-dolt/commands"
)

func main() {
	if _, err := exec.LookPath("dolt"); err != nil {
		log.Fatal("It looks like Dolt is not installed on your system. Make sure that the `dolt` binary is in your PATH before attempting to run git-dolt commands.")
	}

	if filepath.Base(os.Args[0]) == "git-dolt" {
		os.Args = append([]string{"git", "dolt"}, os.Args[1:]...)
	}

	fakeGitCmd := &cobra.Command{
		Use: "git",
	}

	rootCmd := &cobra.Command{
		Use:   "dolt",
		Short: "Run a git-dolt subcommand",
		Long: `Run a git-dolt subcommand.
Valid subcommands are: fetch, install, link, update.`,
	}
	fakeGitCmd.AddCommand(rootCmd)

	cmdInstall := &cobra.Command{
		Use:   "install",
		Short: "Installs the git-dolt smudge filter for this Git repository",
		Long: `Installs the git-dolt smudge filter for this Git repository.
After this, when git-dolt pointer files are checked out in this repository, the corresponding Dolt repositories will automatically be cloned.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return commands.Install()
		},
	}

	cmdLink := &cobra.Command{
		Use:   "link <remote-url>",
		Short: "Links the given Dolt repository to the current Git repository",
		Long: `Links the given Dolt repository to the current Git repository.
The Dolt repository is cloned in the current directory and added to ./.gitignore, and a git-dolt pointer file is created.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return commands.Link(args[0])
		},
	}

	cmdFetch := &cobra.Command{
		Use:   "fetch <pointer-file>",
		Short: "Fetches the Dolt repository referred to in the given git-dolt pointer file",
		Long: `Fetches the Dolt repository referred to in the given git-dolt pointer file.
The Dolt repository is cloned to the current directory and checked out to the revision specified in the git-dolt pointer file.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return commands.Fetch(args[0])
		},
	}

	cmdUpdate := &cobra.Command{
		Use:   "update <pointer-file> <revision>",
		Short: "Updates the reference in the given git-dolt pointer file to the given revision",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return commands.Update(args[0], args[1])
		},
	}

	rootCmd.AddCommand(cmdInstall, cmdLink, cmdFetch, cmdUpdate)
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
