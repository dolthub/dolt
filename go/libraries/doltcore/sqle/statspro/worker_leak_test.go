// Copyright 2025 Dolthub, Inc.
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

//go:build unix
// +build unix

package statspro

import (
	"os"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestGCDoesNotLeakFd(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, _ := defaultSetupDetail(t, threads, false, true, true)

	{
		runBlock(t, ctx, sqlEng,
			"create database otherdb",
			"use otherdb",
			"create table t (i int primary key)",
			"insert into t values (0), (1)",
			"call dolt_stats_gc()",
		)

		nextFd := getNextFd(t)
		sanityNextFd := getNextFd(t)
		require.Equal(t, nextFd, sanityNextFd)

		for i := 0; i < 64; i++ {
			runBlock(t, ctx, sqlEng, "call dolt_stats_gc()")
		}

		finalNextFd := getNextFd(t)
		require.Equal(t, nextFd, finalNextFd)
	}
}

func getNextFd(t *testing.T) uintptr {
	f, err := os.Open("/dev/null")
	require.NoError(t, err)
	defer f.Close()
	return f.Fd()
}
