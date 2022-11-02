// Copyright 2021 Dolthub, Inc.
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

package schema

import (
	"fmt"
	"strings"
)

type Check interface {
	Name() string
	Expression() string
	Enforced() bool
}

// CheckCollection is the set of `check` constraints on a table's schema
type CheckCollection interface {
	// AddCheck adds a check to this collection and returns it
	AddCheck(name, expression string, enforce bool) (Check, error)
	// DropCheck removes the check with the name given
	DropCheck(name string) error
	// AllChecks returns all the checks in the collection
	AllChecks() []Check
	// Count returns the size of the collection
	Count() int
}

type check struct {
	name       string
	expression string
	enforced   bool
}

func (c check) Name() string {
	return c.name
}

func (c check) Expression() string {
	return c.expression
}

func (c check) Enforced() bool {
	return c.enforced
}

type checkCollection struct {
	checks []check
}

func (c *checkCollection) AddCheck(name, expression string, enforce bool) (Check, error) {
	for _, chk := range c.checks {
		if strings.ToLower(name) == strings.ToLower(chk.name) {
			// Engine is supposed to enforce this for us, but just in case
			return nil, fmt.Errorf("name %s in use", name)
		}
	}

	newCheck := check{
		name:       name,
		expression: expression,
		enforced:   enforce,
	}
	c.checks = append(c.checks, newCheck)

	return newCheck, nil
}

func (c *checkCollection) DropCheck(name string) error {
	for i, chk := range c.checks {
		if strings.ToLower(name) == strings.ToLower(chk.name) {
			c.checks = append(c.checks[:i], c.checks[i+1:]...)
			return nil
		}
	}
	return nil
}

func (c *checkCollection) AllChecks() []Check {
	checks := make([]Check, len(c.checks))
	for i, check := range c.checks {
		checks[i] = check
	}
	return checks
}

func (c *checkCollection) Count() int {
	return len(c.checks)
}

func NewCheckCollection() CheckCollection {
	return &checkCollection{
		checks: make([]check, 0),
	}
}

func NewCheck(name, expression string, enforced bool) check {
	return check{
		name:       name,
		expression: expression,
		enforced:   enforced,
	}
}
