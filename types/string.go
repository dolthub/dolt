package types

type String interface {
	Blob

	// Slurps the entire string into memory. You obviously don't want to do this if the string might be large.
	String() string
}

func NewString(s string) String {
	return flatString{flatBlob{[]byte(s)}}
}

func StringFromBytes(b []byte) String {
	return flatString{flatBlob{b}}
}
