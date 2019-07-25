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
	"github.com/liquidata-inc/dolt/go/cmd/git-dolt/utils"
)

// Update updates the git-dolt pointer file at the given filename
// to point to the specified revision.
func Update(ptrFname string, revision string) error {
	ptrFname = utils.EnsureSuffix(ptrFname, ".git-dolt")
	c, err := config.Load(ptrFname)
	if err != nil {
		return err
	}

	c.Revision = revision

	if err := config.Write(ptrFname, c.String()); err != nil {
		return err
	}
	fmt.Printf("Updated pointer file %s to revision %s. You should git commit this change.\n", ptrFname, revision)
	return nil
}
