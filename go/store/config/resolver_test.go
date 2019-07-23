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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/osutil"
	"github.com/liquidata-inc/ld/dolt/go/store/spec"
)

const (
	localSpec = nbsSpec
	testDs    = "testds"
)

type testData struct {
	input    string
	expected string
}

var (
	rtestRoot = os.TempDir()

	rtestConfig = &Config{
		"",
		map[string]DbConfig{
			DefaultDbAlias: {Url: localSpec},
		},
		AWSConfig{},
	}

	dbTestsNoAliases = []testData{
		{localSpec, localSpec},
	}

	dbTestsWithAliases = []testData{
		{"", localSpec},
	}
)

func withConfig(t *testing.T) *Resolver {
	return withGivenConfig(rtestConfig, t)
}

func withGivenConfig(config *Config, t *testing.T) *Resolver {
	assert := assert.New(t)
	dir := filepath.Join(rtestRoot, "with-config")
	_, err := config.WriteTo(dir)
	assert.NoError(err, dir)
	assert.NoError(os.Chdir(dir))
	r := NewResolver() // resolver must be created after changing directory
	return r
}

func withoutConfig(t *testing.T) *Resolver {
	assert := assert.New(t)
	dir := filepath.Join(rtestRoot, "without-config")
	assert.NoError(os.MkdirAll(dir, os.ModePerm), dir)
	assert.NoError(os.Chdir(dir))
	r := NewResolver() // resolver must be created after changing directory
	return r
}

func assertPathSpecsEquiv(assert *assert.Assertions, expected string, actual string) {
	e, err := spec.ForPath(expected)
	assert.NoError(err)
	a, err := spec.ForPath(actual)
	assert.NoError(err)

	databaseSpec := func(sp spec.Spec) string {
		return fmt.Sprintf("%s:%s", sp.Protocol, sp.DatabaseName)
	}

	assertDbSpecsEquiv(assert, databaseSpec(e), databaseSpec(a))
	assert.Equal(e.Path.String(), a.Path.String())
}

func TestResolveDatabaseWithConfig(t *testing.T) {
	if osutil.IsWindows {
		t.Skip("Skipping test as it is specific to Unix")
	}
	r := withConfig(t)
	assert := assert.New(t)
	for _, d := range append(dbTestsNoAliases, dbTestsWithAliases...) {
		db := r.ResolveDbSpec(d.input)
		assertDbSpecsEquiv(assert, d.expected, db)
	}
}

func TestResolveDatabaseWithoutConfig(t *testing.T) {
	r := withoutConfig(t)
	assert := assert.New(t)
	for _, d := range dbTestsNoAliases {
		db := r.ResolveDbSpec(d.input)
		assert.Equal(d.expected, db, d.input)
	}
}

func TestPathResolutionWhenSeparatorMissing(t *testing.T) {
	rConfig := withGivenConfig(rtestConfig, t)
	rNoConfig := withoutConfig(t)
	assert := assert.New(t)

	assert.Equal(rConfig.ResolvePathSpec("db"), rNoConfig.ResolvePathSpec("db"))
	assert.Equal(spec.Separator+"branch", rNoConfig.ResolvePathSpec(spec.Separator+"branch"))
	assertPathSpecsEquiv(assert, localSpec+spec.Separator+"branch", rConfig.ResolvePathSpec(spec.Separator+"branch"))
}

func TestResolveDestPathWithDot(t *testing.T) {
	if osutil.IsWindows {
		t.Skip("Skipping test as it is specific to Unix")
	}
	r := withConfig(t)
	assert := assert.New(t)

	data := []struct {
		src     string
		dest    string
		expSrc  string
		expDest string
	}{
		{"::" + testDs, nbsSpec + "::.", localSpec + "::" + testDs, nbsSpec + "::" + testDs},
		{nbsSpec + "::" + testDs, "::.", nbsSpec + "::" + testDs, localSpec + "::" + testDs},
	}
	for _, d := range data {
		src := r.ResolvePathSpec(d.src)
		dest := r.ResolvePathSpec(d.dest)
		assertPathSpecsEquiv(assert, d.expSrc, src)
		assertPathSpecsEquiv(assert, d.expDest, dest)
	}
}
