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

package main

import (
	"testing"
)

func TestConfig(t *testing.T) {
	RunTestsFile(t, "tests/sql-server-config.yaml")
}

func TestCluster(t *testing.T) {
	RunTestsFile(t, "tests/sql-server-cluster.yaml")
}

func TestOriginal(t *testing.T) {
	RunTestsFile(t, "tests/sql-server-orig.yaml")
}

func TestTLS(t *testing.T) {
	RunTestsFile(t, "tests/sql-server-tls.yaml")
}
