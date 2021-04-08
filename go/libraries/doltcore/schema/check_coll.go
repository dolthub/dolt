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
	DropCheck(name string) error
	AllChecks() []Check
	Count() int
}

type check struct {
	name string
	expression string
	enforced bool
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
	checks map[string]check
}

func (c checkCollection) AddCheck(name, expression string, enforce bool) (Check, error) {
	if _, ok := c.checks[name]; ok {
		return nil, fmt.Errorf("name %s in use", name)
	}
	c.checks[name] = check{
		name:       name,
		expression: expression,
		enforced:   enforce,
	}

	return c.checks[name], nil
}

func (c checkCollection) DropCheck(name string) error {
	delete(c.checks, name)
	return nil
}

func (c checkCollection) AllChecks() []Check {
	checks := make([]Check, len(c.checks))
	i := 0
	for _, check := range c.checks {
		checks[i] = check
		i++
	}
	return checks
}

func (c checkCollection) Count() int {
	return len(c.checks)
}

func NewCheckCollection() CheckCollection {
	return checkCollection{
		checks: make(map[string]check),
	}
}