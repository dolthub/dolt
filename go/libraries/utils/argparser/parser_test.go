package argparser

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestArgParser(t *testing.T) {
	tests := []struct{
		ap *ArgParser
		args []string
		expectedErr error
		expectedOptions map[string]string
		expectedArgs []string
	}{
		{
			NewArgParser(),
			[]string{},
			nil,
			map[string]string{},
			[]string{},
		},
		{
			NewArgParser(),
			[]string{"arg1", "arg2"},
			nil,
			map[string]string{},
			[]string{"arg1", "arg2"},
		},
		{
			NewArgParser(),
			[]string{"--unknown_flag"},
			UnknownArgumentParam{"unknown_flag"},
			map[string]string{},
			[]string{},
		},
		{
			NewArgParser(),
			[]string{"--help"},
			ErrHelp,
			map[string]string{},
			[]string{},
		},
		{
			NewArgParser(),
			[]string{"-h"},
			ErrHelp,
			map[string]string{},
			[]string{},
		},
		{
			NewArgParser(),
			[]string{"help"},
			nil,
			map[string]string{},
			[]string{"help"},
		},
		{
			NewArgParser().SupportsString("param", "p", "", ""),
			[]string{"--param", "value", "arg1"},
			nil,
			map[string]string{"param": "value"},
			[]string{"arg1"},
		},
	}

	for _, test := range tests {
		apr, err := test.ap.Parse(test.args)
		require.Equal(t, test.expectedErr, err)

		if err == nil {
			assert.Equal(t, test.expectedOptions, apr.options)
			assert.Equal(t, test.expectedArgs, apr.args)
		}
	}
}
