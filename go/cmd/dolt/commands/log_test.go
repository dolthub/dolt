// Copyright 2019 Dolthub, Inc.
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
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/osutil"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/outputpager"
)

func TestLog(t *testing.T) {
	dEnv := createUninitializedEnv()
	err := dEnv.InitRepo(context.Background(), types.Format_Default, "Bill Billerson", "bigbillieb@fake.horse", env.DefaultInitBranch)

	if err != nil {
		t.Error("Failed to init repo")
	}

	cs, _ := doltdb.NewCommitSpec(env.DefaultInitBranch)
	commit, _ := dEnv.DoltDB.Resolve(context.Background(), cs, nil)
	meta, _ := commit.GetCommitMeta(context.Background())
	require.Equal(t, "Bill Billerson", meta.Name)
}

func TestLogSigterm(t *testing.T) {
	if osutil.IsWindows {
		t.Skip("Skipping test as function used is not supported on Windows")
	}

	dEnv := createUninitializedEnv()
	err := dEnv.InitRepo(context.Background(), types.Format_Default, "Bill Billerson", "bigbillieb@fake.horse", env.DefaultInitBranch)

	if err != nil {
		t.Error("Failed to init repo")
	}

	cs, _ := doltdb.NewCommitSpec(env.DefaultInitBranch)
	commit, _ := dEnv.DoltDB.Resolve(context.Background(), cs, nil)
	cMeta, _ := commit.GetCommitMeta(context.Background())
	cHash, _ := commit.HashOf()

	outputpager.SetTestingArg(true)
	defer outputpager.SetTestingArg(false)

	pager := outputpager.Start()
	defer pager.Stop()

	chStr := cHash.String()

	for i := 0; i < 5; i++ {
		pager.Writer.Write([]byte(fmt.Sprintf("\033[1;33mcommit %s \033[0m", chStr)))
		pager.Writer.Write([]byte(fmt.Sprintf("\nAuthor: %s <%s>", cMeta.Name, cMeta.Email)))

		timeStr := cMeta.FormatTS()
		pager.Writer.Write([]byte(fmt.Sprintf("\nDate:  %s", timeStr)))

		formattedDesc := "\n\n\t" + strings.Replace(cMeta.Description, "\n", "\n\t", -1) + "\n\n"
		pager.Writer.Write([]byte(fmt.Sprintf(formattedDesc)))
	}

	process, err := os.FindProcess(syscall.Getpid())
	require.NoError(t, err)

	err = process.Signal(syscall.SIGTERM)
	require.NoError(t, err)
}
