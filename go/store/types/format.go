package types

type NomsBinFormat struct {
	tag *formatTag
}

type formatTag struct{}

var formatTag_7_18 *formatTag = nil
var formatTag_LD_1 = &formatTag{}

var Format_7_18 = &NomsBinFormat{}
var Format_LD_1 = &NomsBinFormat{formatTag_LD_1}

func isFormat_7_18(nbf *NomsBinFormat) bool {
	return nbf.tag == formatTag_7_18
}

func getFormatForVersionString(s string) *NomsBinFormat {
	if s == "7.18" {
		return Format_7_18
	} else if s == "__LD_1__" {
		return Format_LD_1
	} else {
		panic("Unsupported ChunkStore.Version() == " + s)
	}
}
