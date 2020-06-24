// Copyright 2020 Liquidata, Inc.
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

package dfunctions

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression/function"
)

func init() {
	// TODO: fix function registration
	function.Defaults = append(function.Defaults, sql.Function1{Name: HashOfFuncName, Fn: NewHashOf})
	function.Defaults = append(function.Defaults, sql.Function1{Name: CommitFuncName, Fn: NewCommitFunc})
	function.Defaults = append(function.Defaults, sql.Function1{Name: MergeFuncName, Fn: NewMergeFunc})
}
