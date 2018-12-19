package errhand

type VerboseError interface {
	error
	Verbose() string
}

type emptyErr struct{}

func (e emptyErr) Error() string {
	return ""
}

func (e emptyErr) Verbose() string {
	return ""
}

var ErrVerboseEmpty = emptyErr{}
