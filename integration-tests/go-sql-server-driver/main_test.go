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
	"flag"
	"log"
	"os"
	"testing"
)

// We generate various TLS keys and certificates and some JWKS/JWT material
// which the tests reference. We do this once for the test run, because it can
// be expensive, and we expose the location of the generated files through an
// environment variable. dtestutils/sql_server_driver interpolates that
// environment variable into a few fields in the test definition.
//
// It's good enough for now, and it keeps us from checking in certificates or
// JWT which will expire at some point in the future.
func TestMain(m *testing.M) {
	res := func() int {
		old := os.Getenv("TESTGENDIR")
		defer func() {
			os.Setenv("TESTGENDIR", old)
		}()
		gendir, err := os.MkdirTemp(os.TempDir(), "go-sql-server-driver-gen-*")
		if err != nil {
			log.Fatalf("could not create temp dir: %v", err)
		}
		defer os.RemoveAll(gendir)
		err = GenerateTestJWTs(gendir)
		if err != nil {
			log.Fatalf("%v", err)
		}
		err = GenerateX509Certs(gendir)
		if err != nil {
			log.Fatalf("%v", err)
		}
		os.Setenv("TESTGENDIR", gendir)
		flag.Parse()
		return m.Run()
	}()
	os.Exit(res)
}

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
