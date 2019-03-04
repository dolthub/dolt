package schema

type Schema interface {
	GetPKCols() *ColCollection
	GetNonPKCols() *ColCollection
	GetAllCols() *ColCollection
}

func ColFromTag(sch Schema, tag uint64) (Column, bool) {
	return sch.GetAllCols().GetByTag(tag)
}

func ColFromName(sch Schema, name string) (Column, bool) {
	return sch.GetAllCols().GetByName(name)
}

func ExtractAllColNames(sch Schema) map[uint64]string {
	colNames := make(map[uint64]string)
	sch.GetAllCols().ItrUnsorted(func(tag uint64, col Column) (stop bool) {
		colNames[tag] = col.Name
		return false
	})

	return colNames
}
