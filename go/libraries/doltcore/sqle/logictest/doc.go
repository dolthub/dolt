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

// Package logictest contains code to define a harness to execute sqllogictests and a runner to run them, as well as a
// parser to parse the result log of such a test run.
// https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
// A git-hub version of the original sqllogictest with all test cases can be found here:
// https://github.com/gregrahn/sqllogictest
package logictest
