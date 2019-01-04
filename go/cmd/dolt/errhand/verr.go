package errhand

type VerboseError interface {
	error
	Verbose() string
	ShouldPrintUsage() bool
}

type emptyErr struct{}

func (e emptyErr) Error() string {
	return ""
}

func (e emptyErr) Verbose() string {
	return ""
}

func (e emptyErr) ShouldPrintUsage() bool {
	return false
}

var ErrVerboseEmpty = emptyErr{}
