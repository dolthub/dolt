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

import "testing"

func TestConfig(t *testing.T) {
	RunTestsFile(t, "tests/sql-server-config.yaml")
}

func TestJWTAuth(t *testing.T) {
	RunTestsFile(t, "tests/sql-server-jwt-auth.yaml")
}

func TestCluster(t *testing.T) {
	RunTestsFile(t, "tests/sql-server-cluster.yaml")
}

func TestClusterUsersAndGrants(t *testing.T) {
	RunTestsFile(t, "tests/sql-server-cluster-users-and-grants.yaml")
}

func TestRemotesAPI(t *testing.T) {
	RunTestsFile(t, "tests/sql-server-remotesapi.yaml")
}

// TestSingle is a convenience method for running a single test from within an IDE. Unskip and set to the file and name
// of the test you want to debug. See README.md in the `tests` directory for more debugging info.
func TestSingle(t *testing.T) {
	t.Skip()
	RunSingleTest(t, "tests/sql-server-cluster.yaml", "primary comes up and replicates to standby")
}

func TestClusterTLS(t *testing.T) {
	RunTestsFile(t, "tests/sql-server-cluster-tls.yaml")
}

func TestOriginal(t *testing.T) {
	RunTestsFile(t, "tests/sql-server-orig.yaml")
}

func TestTLS(t *testing.T) {
	RunTestsFile(t, "tests/sql-server-tls.yaml")
}

func TestClusterReadOnly(t *testing.T) {
	RunTestsFile(t, "tests/sql-server-cluster-read-only.yaml")
}
