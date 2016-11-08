// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package config

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/testify/assert"
)

const (
	ldbSpec     = "ldb:./local"
	memSpec     = "mem"
	httpSpec    = "http://test.com:8080/foo"
	ldbAbsSpec  = "ldb:/tmp/noms"
	remoteAlias = "origin"
)

var (
	ctestRoot = os.TempDir()

	ldbConfig = &Config{
		"",
		map[string]DbConfig{
			DefaultDbAlias: {ldbSpec},
			remoteAlias:    {httpSpec},
		},
	}

	httpConfig = &Config{
		"",
		map[string]DbConfig{
			DefaultDbAlias: {httpSpec},
			remoteAlias:    {ldbSpec},
		},
	}

	memConfig = &Config{
		"",
		map[string]DbConfig{
			DefaultDbAlias: {memSpec},
			remoteAlias:    {httpSpec},
		},
	}

	ldbAbsConfig = &Config{
		"",
		map[string]DbConfig{
			DefaultDbAlias: {ldbAbsSpec},
			remoteAlias:    {httpSpec},
		},
	}
)

type paths struct {
	home   string
	config string
}

func getPaths(assert *assert.Assertions, base string) paths {
	abs, err := filepath.Abs(ctestRoot)
	assert.NoError(err)
	abs, err = filepath.EvalSymlinks(ctestRoot)
	assert.NoError(err)
	home := filepath.Join(abs, base)
	config := filepath.Join(home, NomsConfigFile)
	return paths{home, config}
}

func qualifyFilePath(assert *assert.Assertions, path string) string {
	p, err := filepath.Abs(path)
	assert.NoError(err)
	return p
}

func assertDbSpecsEquiv(assert *assert.Assertions, expected string, actual string) {
	e, err := spec.ForDatabase(expected)
	assert.NoError(err)
	if e.Protocol != "ldb" {
		assert.Equal(expected, actual)
	} else {
		a, err := spec.ForDatabase(actual)
		assert.NoError(err)
		assert.Equal(e.Protocol, a.Protocol, actual)
		if filepath.IsAbs(e.DatabaseName) {
			assert.Equal(e.DatabaseName, a.DatabaseName, actual)
		} else {
			// If the original path is relative, it will return as absolute.
			// All we do here is ensure that the path suffix is the same.
			eName := strings.TrimPrefix(e.DatabaseName, ".")
			assert.True(strings.HasSuffix(a.DatabaseName, eName),
				"expected: %s; actual: %s", eName, actual)
		}
	}
}

func validateConfig(assert *assert.Assertions, file string, e *Config, a *Config) {
	assert.Equal(qualifyFilePath(assert, file), qualifyFilePath(assert, a.File))
	assert.Equal(len(e.Db), len(a.Db))
	for k, er := range e.Db {
		ar, ok := a.Db[k]
		assert.True(ok)
		assertDbSpecsEquiv(assert, er.Url, ar.Url)
	}
}

func writeConfig(assert *assert.Assertions, c *Config, home string) string {
	file, err := c.WriteTo(home)
	assert.NoError(err, home)
	return file
}

func TestConfig(t *testing.T) {
	assert := assert.New(t)
	path := getPaths(assert, "home")
	writeConfig(assert, ldbConfig, path.home)

	// Test from home
	assert.NoError(os.Chdir(path.home))
	c, err := FindNomsConfig()
	assert.NoError(err, path.config)
	validateConfig(assert, path.config, ldbConfig, c)

	// Test from subdir
	subdir := filepath.Join(path.home, "subdir")
	assert.NoError(os.MkdirAll(subdir, os.ModePerm))
	assert.NoError(os.Chdir(subdir))
	c, err = FindNomsConfig()
	assert.NoError(err, path.config)
	validateConfig(assert, path.config, ldbConfig, c)

	// Test from subdir with intervening .nomsconfig directory
	nomsDir := filepath.Join(subdir, NomsConfigFile)
	err = os.MkdirAll(nomsDir, os.ModePerm)
	assert.NoError(err, nomsDir)
	assert.NoError(os.Chdir(subdir))
	c, err = FindNomsConfig()
	assert.NoError(err, path.config)
	validateConfig(assert, path.config, ldbConfig, c)
}

func TestUnreadableConfig(t *testing.T) {
	assert := assert.New(t)
	path := getPaths(assert, "home.unreadable")
	writeConfig(assert, ldbConfig, path.home)
	assert.NoError(os.Chmod(path.config, 0333)) // write-only
	assert.NoError(os.Chdir(path.home))
	_, err := FindNomsConfig()
	assert.Error(err, path.config)
}

func TestNoConfig(t *testing.T) {
	assert := assert.New(t)
	path := getPaths(assert, "home.none")
	assert.NoError(os.MkdirAll(path.home, os.ModePerm))
	assert.NoError(os.Chdir(path.home))
	_, err := FindNomsConfig()
	assert.Equal(NoConfig, err)
}

func TestBadConfig(t *testing.T) {
	assert := assert.New(t)
	path := getPaths(assert, "home.bad")
	cfile := writeConfig(assert, ldbConfig, path.home)
	// overwrite with something invalid
	assert.NoError(ioutil.WriteFile(cfile, []byte("invalid config"), os.ModePerm))
	assert.NoError(os.Chdir(path.home))
	_, err := FindNomsConfig()
	assert.Error(err, path.config)
}

func TestQualifyingPaths(t *testing.T) {
	assert := assert.New(t)
	path := getPaths(assert, "home")
	assert.NoError(os.Chdir(path.home))

	for _, tc := range []*Config{httpConfig, memConfig, ldbAbsConfig} {
		writeConfig(assert, tc, path.home)
		ac, err := FindNomsConfig()
		assert.NoError(err, path.config)
		validateConfig(assert, path.config, tc, ac)
	}
}

func TestCwd(t *testing.T) {
	assert := assert.New(t)
	cwd, err := os.Getwd()
	assert.NoError(err)
	cwd = filepath.Join(cwd, "test")
	abs, err := filepath.Abs("test")
	assert.NoError(err)

	assert.Equal(cwd, abs)
}
