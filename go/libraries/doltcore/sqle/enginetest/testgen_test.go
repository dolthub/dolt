// Copyright 2022 Dolthub, Inc.
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

package enginetest

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/stretchr/testify/require"
)

func TestGenNewFormatQueryPlans(t *testing.T) {
	// must run with env var: DOLT_DEFAULT_BIN_FORMAT="__DOLT__"
	t.Skip()
	harness := newDoltHarness(t).WithParallelism(1)
	harness.Setup(setup.SimpleSetup...)
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)

	outputPath := filepath.Join(t.TempDir(), "queryPlans.txt")
	f, err := os.Create(outputPath)
	require.NoError(t, err)

	w := bufio.NewWriter(f)
	_, _ = w.WriteString("var NewFormatQueryPlanTests = []queries.QueryPlanTest{\n")
	for _, tt := range queries.PlanTests {
		_, _ = w.WriteString("\t{\n")
		ctx := enginetest.NewContext(harness)
		binder := planbuilder.New(ctx, engine.EngineAnalyzer().Catalog, sql.NewMysqlParser())
		parsed, _, _, qFlags, err := binder.Parse(tt.Query, nil, false)
		require.NoError(t, err)

		node, err := engine.EngineAnalyzer().Analyze(ctx, parsed, nil, qFlags)
		require.NoError(t, err)
		planString := enginetest.ExtractQueryNode(node).String()

		if strings.Contains(tt.Query, "`") {
			_, _ = w.WriteString(fmt.Sprintf(`Query: "%s",`, tt.Query))
		} else {
			_, _ = w.WriteString(fmt.Sprintf("Query: `%s`,", tt.Query))
		}
		_, _ = w.WriteString("\n")

		_, _ = w.WriteString(`ExpectedPlan: `)
		for i, line := range strings.Split(planString, "\n") {
			if i > 0 {
				_, _ = w.WriteString(" + \n")
			}
			if len(line) > 0 {
				_, _ = w.WriteString(fmt.Sprintf(`"%s\n"`, strings.ReplaceAll(line, `"`, `\"`)))
			} else {
				// final line with comma
				_, _ = w.WriteString("\"\",\n")
			}
		}
		_, _ = w.WriteString("\t},\n")
	}
	_, _ = w.WriteString("}")

	_ = w.Flush()

	t.Logf("Query plans in %s", outputPath)
}
