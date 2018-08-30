// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/attic-labs/noms/go/spec"
	"github.com/stretchr/testify/assert"
)

const (
	localSpec  = nbsSpec
	remoteSpec = httpSpec
	testDs     = "testds"
	testObject = "#pckdvpvr9br1fie6c3pjudrlthe7na18"
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
			remoteAlias:    {Url: remoteSpec},
		},
		AWSConfig{},
	}

	testConfigWithOptions = &Config{
		"",
		map[string]DbConfig{
			remoteAlias: {Url: remoteSpec, Options: map[string]string{"param": "value"}},
		},
		AWSConfig{},
	}

	dbTestsNoAliases = []testData{
		{localSpec, localSpec},
		{remoteSpec, remoteSpec},
	}

	dbTestsWithAliases = []testData{
		{"", localSpec},
		{remoteAlias, remoteSpec},
	}

	pathTestsNoAliases = []testData{
		{remoteSpec + "::" + testDs, remoteSpec + "::" + testDs},
		{remoteSpec + "::" + testObject, remoteSpec + "::" + testObject},
	}

	pathTestsWithAliases = []testData{
		{spec.Separator + testDs, localSpec + "::" + testDs},
		{remoteAlias + "::" + testDs, remoteSpec + "::" + testDs},
		{spec.Separator + testObject, localSpec + "::" + testObject},
		{remoteAlias + "::" + testObject, remoteSpec + "::" + testObject},
	}

	pathTestForConfigs = []testData{
		{remoteAlias + "::" + testDs, remoteSpec + "::" + testDs},
		{remoteAlias + "::" + testObject, remoteSpec + "::" + testObject},
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
	r := withConfig(t)
	assert := assert.New(t)
	for _, d := range append(dbTestsNoAliases, dbTestsWithAliases...) {
		db := r.ResolveDbSpec(d.input)
		assertDbSpecsEquiv(assert, d.expected, db)
	}
}

func TestResolvePathWithConfig(t *testing.T) {
	r := withConfig(t)
	assert := assert.New(t)
	for _, d := range append(pathTestsNoAliases, pathTestsWithAliases...) {
		path := r.ResolvePathSpec(d.input)
		assertPathSpecsEquiv(assert, d.expected, path)
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

func TestResolvePathWithoutConfig(t *testing.T) {
	r := withoutConfig(t)
	assert := assert.New(t)
	for _, d := range pathTestsNoAliases {
		path := r.ResolvePathSpec(d.input)
		assertPathSpecsEquiv(assert, d.expected, path)
	}
}

func TestDbConfigOptions(t *testing.T) {
	r := withGivenConfig(rtestConfig, t)
	assert := assert.New(t)

	dbConfig := r.DbConfigForDbSpec(remoteAlias)
	assert.Zero(dbConfig.Options)
	for _, d := range pathTestForConfigs {
		db, dbConfig := r.ResolvePathSpecAndGetDbConfig(d.input)
		assertPathSpecsEquiv(assert, d.expected, db)
		assert.Zero(dbConfig.Options)
	}

	r = withGivenConfig(testConfigWithOptions, t)
	dbConfig = r.DbConfigForDbSpec(remoteAlias)
	assert.Equal(1, len(dbConfig.Options))
	assert.Equal("value", dbConfig.Options["param"])
	for _, d := range pathTestForConfigs {
		db, dbConfig := r.ResolvePathSpecAndGetDbConfig(d.input)
		assertPathSpecsEquiv(assert, d.expected, db)
		assert.Equal(1, len(dbConfig.Options))
		assert.Equal("value", dbConfig.Options["param"])
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
	r := withConfig(t)
	assert := assert.New(t)

	data := []struct {
		src     string
		dest    string
		expSrc  string
		expDest string
	}{
		{"::" + testDs, remoteSpec + "::.", localSpec + "::" + testDs, remoteSpec + "::" + testDs},
		{remoteSpec + "::" + testDs, "::.", remoteSpec + "::" + testDs, localSpec + "::" + testDs},
	}
	for _, d := range data {
		src := r.ResolvePathSpec(d.src)
		dest := r.ResolvePathSpec(d.dest)
		assertPathSpecsEquiv(assert, d.expSrc, src)
		assertPathSpecsEquiv(assert, d.expDest, dest)
	}

}
