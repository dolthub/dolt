package types

import "errors"
import "os"
import "github.com/liquidata-inc/ld/dolt/go/store/constants"

type NomsBinFormat struct {
	tag *formatTag
}

type formatTag struct{}

var formatTag_7_18 *formatTag = nil
var formatTag_LD_1 = &formatTag{}

var Format_7_18 = &NomsBinFormat{}
var Format_LD_1 = &NomsBinFormat{formatTag_LD_1}

var Format_Default *NomsBinFormat

func isFormat_7_18(nbf *NomsBinFormat) bool {
	return nbf.tag == formatTag_7_18
}

func GetFormatForVersionString(s string) (*NomsBinFormat, error) {
	if s == constants.Format718String {
		return Format_7_18, nil
	} else if s == constants.FormatLD1String {
		return Format_LD_1, nil
	} else {
		return nil, errors.New("unsupported ChunkStore version " + s)
	}
}

func (nbf *NomsBinFormat) VersionString() string {
	if nbf.tag == formatTag_7_18 {
		return constants.Format718String
	} else if nbf.tag == formatTag_LD_1 {
		return constants.FormatLD1String
	} else {
		panic("unrecognized NomsBinFormat tag value")
	}
}

func init() {
	nbfVerStr := os.Getenv("DOLT_DEFAULT_BIN_FORMAT")
	if nbfVerStr == "" {
		nbfVerStr = constants.FormatDefaultString
	}
	nbf, err := GetFormatForVersionString(nbfVerStr)
	if err != nil {
		panic("unrecognized value for DOLT_DEFAULT_BIN_FORMAT " + nbfVerStr)
	}
	Format_Default = nbf
}
