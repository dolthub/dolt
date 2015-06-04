package types

type flatString struct {
	flatBlob
}

func (fs flatString) String() string {
	return string(fs.data)
}
