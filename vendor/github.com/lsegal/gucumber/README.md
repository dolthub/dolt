# Gucumber

[![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg)](http://godoc.org/github.com/lsegal/gucumber)
[![Build Status](https://img.shields.io/travis/lsegal/gucumber.svg)](https://travis-ci.org/lsegal/gucumber)
[![MIT License](http://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/lsegal/gucumber/blob/master/LICENSE.txt)

An implementation of [Cucumber][cuke] BDD-style testing for Go.

# Installing

```sh
$ go get github.com/lsegal/gucumber/cmd/gucumber
```

# Usage

Cucumber tests are made up of plain text ".feature" files and program source
"step definitions", which for Gucumber are written in Go.

## Features

Put [feature files][features] `internal/features/` with whatever organization you
prefer. For example, you might create `internal/features/accounts/login.feature`
with the following text:

```
@login
Feature: Login Support

  Scenario: User successfully logs in
    Given I have user/pass "foo" / "bar"
    And they log into the website with user "foo" and password "bar"
    Then the user should be successfully logged in
```

## Step Definitions

Create step definitions to match each step in your feature files. These go
in ".go" files in the same `internal/features/` directory. We might create
`internal/features/accounts/step_definitions.go`:

```go
package accounts

import (
	. "github.com/lsegal/gucumber"
)

func init() {
	user, pass := "", ""

	Before("@login", func() {
		// runs before every feature or scenario tagged with @login
		generatePassword()
	})

	Given(`^I have user/pass "(.+?)" / "(.+?)"$`, func(u, p string) {
		user, pass = u, p
	})

	// ...

	Then(`^the user should be successfully logged in$`, func() {
		if !userIsLoggedIn() {
			T.Fail("user should have been logged in")
		}
	})
}
```

### T?

The `T` value is a [testing.T](http://golang.org/pkg/testing/#T) style
value that represents the test context for each test. It mostly supports
`Errorf(fmt, args...)`, but also supports other convenience methods. See
 the [API documentation](http://godoc.org/github.com/lsegal/gucumber#TestingT)
 for more information.

## Running

To run your tests, execute:

```sh
$ gucumber
```

You can also specify the path to features in command line arguments:

```sh
$ gucumber path/to/features
```

You can also filter features and scenarios by tags:

```sh
$ gucumber -tags=@login # only run login feature(s)
```

Or:

```sh
$ gucumber -tags=~@slow # ignore all "slow" scenarios
```

# Copyright

This library was written by [Loren Segal][lsegal] in 2015. It is licensed for
use under the [MIT license][mit].

[cuke]: http://cukes.info
[features]: https://github.com/cucumber/cucumber/wiki/Feature-Introduction
[lsegal]: http://gnuu.org
[mit]: http://opensource.org/licenses/MIT
