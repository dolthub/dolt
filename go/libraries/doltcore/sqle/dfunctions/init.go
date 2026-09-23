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
	sql.NewFunction1(HashOfFuncName, NewHashOfFunc(HashOfFuncName)),
	sql.NewFunction1(DeprecatedHashOfFuncName, NewHashOfFunc(DeprecatedHashOfFuncName)),
	sql.NewFunction0(VersionFuncName, NewVersion),
	sql.NewFunction0(StorageFormatFuncName, NewStorageFormat),
	sql.NewFunction0(ActiveBranchFuncName, NewActiveBranchFunc),
	sql.NewFunction2(DoltMergeBaseFuncName, NewMergeBase),
	sql.NewFunction2(HasAncestorFuncName, NewHasAncestor),
	sql.NewFunction1(HashOfTableFuncName, NewHashOfTable),
	sql.NewFunctionN(HashOfDatabaseFuncName, NewHashOfDatabase),
	sql.NewFunction1(JoinCostFuncName, NewJoinCost),
}

// DolthubApiFunctions are the DoltFunctions that get exposed to Dolthub Api.
var DolthubApiFunctions = []sql.Function{
	sql.NewFunction1(HashOfFuncName, NewHashOfFunc(HashOfFuncName)),
	sql.NewFunction1(DeprecatedHashOfFuncName, NewHashOfFunc(DeprecatedHashOfFuncName)),
	sql.NewFunction0(VersionFuncName, NewVersion),
	sql.NewFunction0(StorageFormatFuncName, NewStorageFormat),
	sql.NewFunction0(ActiveBranchFuncName, NewActiveBranchFunc),
	sql.NewFunction2(DoltMergeBaseFuncName, NewMergeBase),
}
