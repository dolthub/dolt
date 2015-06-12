package types

type String interface {
	Value

	Blob() Blob

	// Slurps the entire string into memory. You obviously don't want to do this if the string might be large.
	String() string
}

func NewString(s string) String {
	return flatString{s, &cachedRef{}}
}
