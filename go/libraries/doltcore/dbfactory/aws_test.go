package dbfactory

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAWSPathValidation(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		expectedPath string
		expectErr    bool
	}{
		{
			"empty path",
			"",
			"",
			true,
		},
		{
			"basic",
			"database",
			"database",
			false,
		},
		{
			"slash prefix",
			"/database",
			"database",
			false,
		},
		{
			"slash suffix",
			"database/",
			"database",
			false,
		},
		{
			"slash prefix and suffix",
			"/database/",
			"database",
			false,
		},
		{
			"slash in the middle",
			"/data/base/",
			"",
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualPath, actualErr := validatePath(test.path)

			assert.Equal(t, actualPath, test.expectedPath)

			if test.expectErr {
				assert.Error(t, actualErr, "Did not expect an error")
			} else {
				assert.NoError(t, actualErr, "Expected an error")
			}
		})
	}
}
