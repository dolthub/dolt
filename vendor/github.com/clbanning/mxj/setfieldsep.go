package mxj

// Per: https://github.com/clbanning/mxj/issues/37#issuecomment-278651862
var fieldSep string = ":"

// SetFieldSeparator changes the default field separator, ":", for the
// newVal argument in mv.UpdateValuesForPath and the optional 'subkey' arguments
// in mv.ValuesForKey and mv.ValuesForPath. 
// 
// E.g., if the newVal value is "http://blah/blah", setting the field separator
// to "|" will allow the newVal specification, "<key>|http://blah/blah" to parse
// properly.  If called with no argument or an empty string value, the field
// separator is set to the default, ":".
func SetFieldSeparator(s ...string) {
	if len(s) == 0 || s[0] == "" {
		fieldSep = ":" // the default
		return
	}
	fieldSep = s[0]
}
