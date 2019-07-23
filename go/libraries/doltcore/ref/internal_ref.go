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

import "strings"

// InternalRef is a dolt internal reference
type InternalRef struct {
	path string
}

// GetType returns InternalRefType
func (ir InternalRef) GetType() RefType {
	return InternalRefType
}

// GetPath returns the name of the internal reference
func (ir InternalRef) GetPath() string {
	return ir.path
}

// String returns the fully qualified reference e.g. refs/internal/create
func (ir InternalRef) String() string {
	return String(ir)
}

// NewInternalRef creates an internal ref
func NewInternalRef(name string) DoltRef {
	if IsRef(name) {
		prefix := PrefixForType(InternalRefType)
		if strings.HasPrefix(name, prefix) {
			name = name[len(prefix):]
		} else {
			panic(name + " is a ref that is not of type " + prefix)
		}
	}

	return InternalRef{name}
}
