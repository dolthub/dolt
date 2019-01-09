package pipeline

type NamedTransform struct {
	Name string
	Func TransformFunc
}

type TransformCollection struct {
	Transforms []NamedTransform
}

func NewTransformCollection(namedTransforms ...NamedTransform) *TransformCollection {
	return &TransformCollection{namedTransforms}
}

func (tc *TransformCollection) AppendTransforms(nt NamedTransform) {
	tc.Transforms = append(tc.Transforms, nt)
}

func (tc *TransformCollection) NumTransforms() int {
	return len(tc.Transforms)
}

func (tc *TransformCollection) TransformAt(idx int) NamedTransform {
	return tc.Transforms[idx]
}
