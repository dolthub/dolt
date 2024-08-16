// Copyright 2019 Dolthub, Inc.
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

package doltdb

import (
	"context"
)

type DoltCITablesCreator interface {
	CreateTables(ctx context.Context, rv RootValue) (RootValue, error)
}

type doltCITablesCreator struct {
	workflowsTC      DoltCITableCreator
	workflowEventsTC DoltCITableCreator
}

func NewDoltCITablesCreator(dbName string) *doltCITablesCreator {
	return &doltCITablesCreator{
		workflowsTC:      NewDoltCIWorkflowsTableCreator(),
		workflowEventsTC: NewDoltCIWorkflowEventsTableCreator(dbName),
	}
}

func (d doltCITablesCreator) CreateTables(ctx context.Context, rv RootValue) (RootValue, error) {
	rv, err := d.workflowsTC.CreateTable(ctx, rv)
	if err != nil {
		return nil, err
	}

	return d.workflowEventsTC.CreateTable(ctx, rv)
}

var _ DoltCITablesCreator = &doltCITablesCreator{}
