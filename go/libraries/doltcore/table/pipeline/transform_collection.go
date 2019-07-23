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

package pipeline

// TransformCollection is a collection of transforms to be applied in order in a pipeline
type TransformCollection struct {
	// Transforms is a slice of named transforms stored in the order they will be applied
	Transforms []NamedTransform
}

// NewTransformCollection creates a TransformCollection from NamedTransforms
func NewTransformCollection(namedTransforms ...NamedTransform) *TransformCollection {
	return &TransformCollection{namedTransforms}
}

// AppendTransform will mutate the internal slice of transforms by appending this new transform to the slice of
// Transforms
func (tc *TransformCollection) AppendTransforms(nt NamedTransform) {
	tc.Transforms = append(tc.Transforms, nt)
}

// NumTransforms returns the number of NamedTransforms in the collection
func (tc *TransformCollection) NumTransforms() int {
	return len(tc.Transforms)
}

// TransformAt returns the NamedTransform at a given index
func (tc *TransformCollection) TransformAt(idx int) NamedTransform {
	return tc.Transforms[idx]
}
