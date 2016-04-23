package types

// FixupType goes trough the object graph of tr and updates the PackageRef to pkg if the the old PackageRef was an empty ref.
func FixupType(tr *Type, pkg *Package) *Type {
	switch desc := tr.Desc.(type) {
	case PrimitiveDesc:
		return tr

	case CompoundDesc:
		elemTypes := make([]*Type, len(desc.ElemTypes))
		for i, elemType := range desc.ElemTypes {
			elemTypes[i] = FixupType(elemType, pkg)
		}
		return makeCompoundType(tr.Kind(), elemTypes...)

	case UnresolvedDesc:
		if tr.HasPackageRef() {
			return tr
		}
		return MakeType(pkg.Ref(), tr.Ordinal())

	case StructDesc:
		fixField := func(f Field) Field {
			newT := FixupType(f.T, pkg)
			return Field{Name: f.Name, T: newT, Optional: f.Optional}
		}
		fixFields := func(fields []Field) []Field {
			newFields := make([]Field, len(fields))
			for i, f := range fields {
				newFields[i] = fixField(f)
			}
			return newFields
		}
		return MakeStructType(tr.Name(), fixFields(desc.Fields), fixFields(desc.Union))
	}

	panic("unreachable")
}
