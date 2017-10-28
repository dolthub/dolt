package host

import (
	"testing"
)

func TestSemverMatching(t *testing.T) {
	m, err := MultistreamSemverMatcher("/testing/4.3.5")
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string]bool{
		"/testing/4.3.0":   true,
		"/testing/4.3.7":   true,
		"/testing/4.3.5":   true,
		"/testing/4.2.7":   true,
		"/testing/4.0.0":   true,
		"/testing/5.0.0":   false,
		"/cars/dogs/4.3.5": false,
		"/foo/1.0.0":       false,
		"":                 false,
		"dogs":             false,
		"/foo":             false,
		"/foo/1.1.1.1":     false,
	}

	for p, ok := range cases {
		if m(p) != ok {
			t.Fatalf("expected %s to be %t", p, ok)
		}
	}
}
