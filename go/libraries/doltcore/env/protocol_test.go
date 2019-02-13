package env

import "testing"

func TestHasSchemaRegex(t *testing.T) {
	tests := []struct {
		in       string
		expected bool
	}{
		{"", false},
		{"a", false},
		{"ab://", true},
		{" ab://", false},
		{" ab#://", false},
		{"ab://#Any thing after ://", true},
		{"aB2.3-4+://", true},
	}

	for _, test := range tests {
		if hasSchemaRegEx.MatchString(test.in) != test.expected {
			t.Error("Unexpected result for", test.in)
		}
	}
}
