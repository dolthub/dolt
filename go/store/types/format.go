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

func getFormatForVersionString(s string) *Format {
	if s == "7.18" {
		return Format_7_18
	} else if s == "__LD_1__" {
		return Format_LD_1
	} else {
		panic("Unsupported ChunkStore.Version() == " + s)
	}
}
