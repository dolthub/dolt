// Copyright 2021 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
)

type Test struct{}

func (t Test) Name() string {
	return "test"
}

func (t Test) Description() string {
	panic("test command")
}

func (t Test) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return 1
	}

	wsRef := ref.NewWorkingSetRef("test-workingset4")
	err = dEnv.DoltDB.UpdateWorkingSet(ctx, wsRef, root, hash.Hash{})
	return HandleVErrAndExitCode(errhand.BuildDError("oopsie").AddCause(err).Build(), nil)
}

func (t Test) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	return nil
}
