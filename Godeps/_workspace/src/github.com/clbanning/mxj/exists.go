package mxj

// Checks whether the path exists
func (mv Map) Exists(path string) bool {
	v, err := mv.ValuesForPath(path)
	return err == nil && len(v) > 0
}
