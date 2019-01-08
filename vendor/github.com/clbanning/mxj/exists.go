package mxj

// Checks whether the path exists
func (mv Map) Exists(path string, subkeys ...string) bool {
	v, err := mv.ValuesForPath(path, subkeys...)
	return err == nil && len(v) > 0
}
