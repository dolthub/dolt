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
