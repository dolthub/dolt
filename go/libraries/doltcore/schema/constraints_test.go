package schema

import "testing"

func TestConstraintTypes(t *testing.T) {
	tests := map[string]ConstraintType {
		"primary_key": PrimaryKey,
		"nonsense string": Invalid,
	}

	for str, expected := range tests {
		if ConstraintFromString(str) != expected {
			t.Error(str, "did not map to the expected value")
		}
	}

	reverseTests := map[ConstraintType]string {
		PrimaryKey: "primary_key",
		Invalid: "invalid",
	}

	for c, expectedStr := range reverseTests {
		if c.String() != expectedStr {
			t.Error(c.String(), "Is not the expected string for this constraint")
		}
	}
}
