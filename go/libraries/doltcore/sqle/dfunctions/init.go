// Copyright 2020 Dolthub, Inc.
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

import "github.com/dolthub/go-mysql-server/sql"

var DoltFunctions = []sql.Function{
	sql.Function1{Name: HashOfFuncName, Fn: NewHashOf},
	sql.FunctionN{Name: CommitFuncName, Fn: NewCommitFunc},
	sql.FunctionN{Name: MergeFuncName, Fn: NewMergeFunc},
	sql.Function0{Name: VersionFuncName, Fn: NewVersion},
	sql.FunctionN{Name: DoltCommitFuncName, Fn: NewDoltCommitFunc},
	sql.FunctionN{Name: DoltAddFuncName, Fn: NewDoltAddFunc},
	sql.FunctionN{Name: DoltCleanFuncName, Fn: NewDoltCleanFunc},
	sql.FunctionN{Name: DoltResetFuncName, Fn: NewDoltResetFunc},
	sql.FunctionN{Name: DoltCheckoutFuncName, Fn: NewDoltCheckoutFunc},
	sql.FunctionN{Name: DoltMergeFuncName, Fn: NewDoltMergeFunc},
	sql.Function0{Name: ActiveBranchFuncName, Fn: NewActiveBranchFunc},
	sql.Function2{Name: DoltMergeBaseFuncName, Fn: NewMergeBase},
	sql.FunctionN{Name: ConstraintsVerifyFuncName, Fn: NewConstraintsVerifyFunc},
	sql.FunctionN{Name: ConstraintsVerifyAllFuncName, Fn: NewConstraintsVerifyAllFunc},
	sql.FunctionN{Name: RevertFuncName, Fn: NewRevertFunc},
	sql.FunctionN{Name: DoltPullFuncName, Fn: NewPullFunc},
	sql.FunctionN{Name: DoltFetchFuncName, Fn: NewFetchFunc},
	sql.FunctionN{Name: DoltPushFuncName, Fn: NewPushFunc},
	sql.FunctionN{Name: DoltBranchFuncName, Fn: NewDoltBranchFunc},
	sql.FunctionN{Name: DoltBackupFuncName, Fn: NewDoltBackupFunc},
}

// DolthubApiFunctions are the DoltFunctions that get exposed to Dolthub Api.
var DolthubApiFunctions = []sql.Function{
	sql.Function1{Name: HashOfFuncName, Fn: NewHashOf},
	sql.Function0{Name: VersionFuncName, Fn: NewVersion},
	sql.Function0{Name: ActiveBranchFuncName, Fn: NewActiveBranchFunc},
	sql.Function2{Name: DoltMergeBaseFuncName, Fn: NewMergeBase},
}
