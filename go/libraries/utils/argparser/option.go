package argparser

import (
	"github.com/pkg/errors"
	"strconv"
)

type OptionType int

const (
	OptionalFlag OptionType = iota
	OptionalValue
)

type ValidationFunc func(string) error

func isIntStr(str string) error {
	_, err := strconv.ParseInt(str, 10, 32)
	if err != nil {
		return errors.New("error: \"" + str + "\" is not a valid int.")
	}

	return nil
}

func isUintStr(str string) error {
	_, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return errors.New("error: \"" + str + "\" is not a valid int.")
	}

	return nil
}

type Option struct {
	Name      string
	Abbrev    string
	ValDesc   string
	OptType   OptionType
	Desc      string
	Validator ValidationFunc
}
