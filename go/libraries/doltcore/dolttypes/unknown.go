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

package dolttypes

import (
	"fmt"
)

type Unknown struct{}

func (v Unknown) Compare(other DoltType) int {
	return 0
}

func (v Unknown) Decode([]byte) (DoltType, error) {
	return Unknown{}, fmt.Errorf("DoltType unknown was created")
}

func (v Unknown) Encode() ([]byte, error) {
	return nil, fmt.Errorf("cannot encode the unknown type")
}

func (v Unknown) Equals(other DoltType) bool {
	return true
}

func (v Unknown) Kind() DoltKind {
	return UnknownKind
}

func (v Unknown) MarshalBool() (bool, error) {
	return false, fmt.Errorf("cannot serialize the unknown type")
}

func (v Unknown) MarshalDoltType(DoltKind) (DoltType, error) {
	return nil, fmt.Errorf("cannot serialize the unknown type")
}

func (v Unknown) MarshalFloat() (float64, error) {
	return 0, fmt.Errorf("cannot serialize the unknown type")
}

func (v Unknown) MarshalInt() (int64, error) {
	return 0, fmt.Errorf("cannot serialize the unknown type")
}

func (v Unknown) MarshalString() (string, error) {
	return "", fmt.Errorf("cannot serialize the unknown type")
}

func (v Unknown) MarshalUint() (uint64, error) {
	return 0, fmt.Errorf("cannot serialize the unknown type")
}

func (v Unknown) UnmarshalBool(bool) (DoltType, error) {
	return nil, fmt.Errorf("cannot serialize the unknown type")
}

func (v Unknown) UnmarshalDoltType(DoltType) (DoltType, error) {
	return nil, fmt.Errorf("cannot serialize the unknown type")
}

func (v Unknown) UnmarshalFloat(fl float64) (DoltType, error) {
	return nil, fmt.Errorf("cannot serialize the unknown type")
}

func (v Unknown) UnmarshalInt(n int64) (DoltType, error) {
	return nil, fmt.Errorf("cannot serialize the unknown type")
}

func (v Unknown) UnmarshalString(s string) (DoltType, error) {
	return nil, fmt.Errorf("cannot serialize the unknown type")
}

func (v Unknown) UnmarshalUint(n uint64) (DoltType, error) {
	return nil, fmt.Errorf("cannot serialize the unknown type")
}

func (v Unknown) String() string {
	return "unknown"
}