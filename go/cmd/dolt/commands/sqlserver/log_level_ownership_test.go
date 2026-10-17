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

package sqlserver

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

func TestLogLevelValuesAreEngineLocalButLoggerIsProcessWide(t *testing.T) {
	previousLevel := logrus.GetLevel()
	t.Cleanup(func() { logrus.SetLevel(previousLevel) })
	a, b := sqle.NewEngineVariables(), sqle.NewEngineVariables()
	a.System.AddSystemVariables([]sql.SystemVariable{newLogLevelSystemVariable("info")})
	b.System.AddSystemVariables([]sql.SystemVariable{newLogLevelSystemVariable("info")})
	ctxA := sql.NewContextWithVariables(context.Background(), a)
	ctxB := sql.NewContextWithVariables(context.Background(), b)

	require.NoError(t, a.System.SetGlobal(ctxA, dsess.DoltLogLevel, "debug"))
	_, levelA, _ := a.System.GetGlobal(dsess.DoltLogLevel)
	_, levelB, _ := b.System.GetGlobal(dsess.DoltLogLevel)
	require.Equal(t, "debug", levelA)
	require.Equal(t, "info", levelB)
	require.Equal(t, logrus.DebugLevel, logrus.GetLevel())

	require.NoError(t, b.System.SetGlobal(ctxB, dsess.DoltLogLevel, logrus.WarnLevel.String()))
	_, levelA, _ = a.System.GetGlobal(dsess.DoltLogLevel)
	require.Equal(t, "debug", levelA)
	require.Equal(t, logrus.WarnLevel, logrus.GetLevel())
}
