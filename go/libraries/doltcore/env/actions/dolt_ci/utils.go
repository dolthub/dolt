// Copyright 2025 Dolthub, Inc.
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

package dolt_ci

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func ParseSavedQueryExpectedResultString(str string) (WorkflowSavedQueryExpectedRowColumnComparisonType, int64, error) {
	if str == "" {
		return WorkflowSavedQueryExpectedRowColumnComparisonTypeUnspecified, 0, nil
	}

	parts := strings.Split(strings.TrimSpace(str), " ")
	if len(parts) == 1 {
		i, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		return WorkflowSavedQueryExpectedRowColumnComparisonTypeEquals, i, nil
	}
	if len(parts) == 2 {
		i, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		switch strings.TrimSpace(parts[0]) {
		case "==":
			return WorkflowSavedQueryExpectedRowColumnComparisonTypeEquals, i, nil
		case "!=":
			return WorkflowSavedQueryExpectedRowColumnComparisonTypeNotEquals, i, nil
		case ">":
			return WorkflowSavedQueryExpectedRowColumnComparisonTypeGreaterThan, i, nil
		case ">=":
			return WorkflowSavedQueryExpectedRowColumnComparisonTypeGreaterThanOrEqual, i, nil
		case "<":
			return WorkflowSavedQueryExpectedRowColumnComparisonTypeLessThan, i, nil
		case "<=":
			return WorkflowSavedQueryExpectedRowColumnComparisonTypeLessThanOrEqual, i, nil
		default:
			return 0, 0, errors.New("unknown comparison type")
		}
	}
	return 0, 0, fmt.Errorf("unable to parse comparison string: %s", str)
}

func ValidateQueryExpectedRowOrColumnCount(countReal int64, countExpected int64, comp WorkflowSavedQueryExpectedRowColumnComparisonType, RowColumnType string) error {
	switch comp {
	case WorkflowSavedQueryExpectedRowColumnComparisonTypeEquals:
		if countReal != countExpected {
			return fmt.Errorf("expected %s count %d, got %d", RowColumnType, countExpected, countReal)
		}
	case WorkflowSavedQueryExpectedRowColumnComparisonTypeNotEquals:
		if countReal == countExpected {
			return fmt.Errorf("expected %s count not %d and got %d", RowColumnType, countExpected, countReal)
		}
	case WorkflowSavedQueryExpectedRowColumnComparisonTypeGreaterThan:
		if countReal <= countExpected {
			return fmt.Errorf("expected %s count greater than %d, got %d", RowColumnType, countExpected, countReal)
		}
	case WorkflowSavedQueryExpectedRowColumnComparisonTypeGreaterThanOrEqual:
		if countReal < countExpected {
			return fmt.Errorf("expected %s count greater than or equal to %d, got %d", RowColumnType, countExpected, countReal)
		}
	case WorkflowSavedQueryExpectedRowColumnComparisonTypeLessThan:
		if countReal >= countExpected {
			return fmt.Errorf("expected %s count less than %d, got %d", RowColumnType, countExpected, countReal)
		}
	case WorkflowSavedQueryExpectedRowColumnComparisonTypeLessThanOrEqual:
		if countReal > countExpected {
			return fmt.Errorf("expected %s count less than or equal to %d, got %d", RowColumnType, countExpected, countReal)
		}
	default:
		return fmt.Errorf("no assertion run")
	}

	return nil
}
