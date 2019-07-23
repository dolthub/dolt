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

package ref

// MarshalableRef is a wrapper that provides the marshaling and unmarshaling of DoltRefs as strings within json.
type MarshalableRef struct {
	Ref DoltRef
}

// MarshalJSON marshal the ref as a string
func (mr MarshalableRef) MarshalJSON() ([]byte, error) {
	if mr.Ref == nil {
		return []byte{}, nil
	}

	return MarshalJSON(mr.Ref)
}

// UnmarshalJSON unmarshals the ref from a string
func (mr *MarshalableRef) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	dref, err := Parse(string(data[1 : len(data)-1]))

	if err != nil {
		return err
	}

	mr.Ref = dref

	return nil
}
