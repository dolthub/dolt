package doltdb

import (
	"reflect"
	"testing"
)

func TestParseInstructions(t *testing.T) {
	tests := []struct {
		inputStr  string
		expected  []int
		expectErr bool
	}{
		{"", []int{}, false},
		{"^", []int{0}, false},
		{"^1", []int{0}, false},
		{"~", []int{0}, false},
		{"~1", []int{0}, false},
		{"^10", []int{9}, false},
		{"~3", []int{0, 0, 0}, false},
		{"^^", []int{0, 0}, false},
		{"^2~3^5", []int{1, 0, 0, 0, 4}, false},
		{"invalid", nil, true},
	}

	for _, test := range tests {
		actual, actualErr := parseInstructions(test.inputStr)

		if actualErr != nil {
			if !test.expectErr {
				t.Error(test.inputStr, "- unexpected err")
			}
		} else if !reflect.DeepEqual(test.expected, actual) {
			t.Error("Error parsing", test.inputStr)
		}
	}
}

func TestSplitAnscestorSpec(t *testing.T) {
	tests := []struct {
		inputStr         string
		expectedCSpecStr string
		expectedASpecStr string
		expectErr        bool
	}{
		{"master", "master", "", false},
		{"MASTER^1", "MASTER", "^1", false},
		{"head~3^^", "head", "~3^^", false},
		{"HEAD~3^^", "HEAD", "~3^^", false},
		{"branch^invalid", "", "", true},
	}

	for _, test := range tests {
		actualCSpecStr, actualASpec, actualErr := SplitAncestorSpec(test.inputStr)

		if actualErr != nil {
			if !test.expectErr {
				t.Error(test.inputStr, "- unexpected err")
			}
		} else if actualCSpecStr != test.expectedCSpecStr {
			t.Error(test.inputStr, "- actual commit spcec:", actualCSpecStr, "expected commit spec:", test.expectedCSpecStr)
		} else if actualASpec.SpecStr != test.expectedASpecStr {
			t.Error(test.inputStr, "- actual commit spcec:", actualCSpecStr, "expected commit spec:", test.expectedCSpecStr)
		}
	}
}
