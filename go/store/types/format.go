package types

type Format struct {
	tag *formatTag
}

type formatTag struct{}

var formatTag_7_18 *formatTag = nil
var formatTag_LD_1 = &formatTag{}

var Format_7_18 = &Format{}
var Format_LD_1 = &Format{formatTag_LD_1}

func isFormat_7_18(f *Format) bool {
	return f.tag == formatTag_7_18
}
