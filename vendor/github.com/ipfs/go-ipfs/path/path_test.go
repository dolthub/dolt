package path

import (
	"testing"
)

func TestPathParsing(t *testing.T) {
	cases := map[string]bool{
		"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n":             true,
		"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a":           true,
		"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b/c/d/e/f": true,
		"/ipns/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b/c/d/e/f": true,
		"/ipns/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n":             true,
		"QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b/c/d/e/f":       true,
		"QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n":                   true,
		"/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n":                  false,
		"/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a":                false,
		"/ipfs/": false,
		"ipfs/":  false,
		"ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n": false,
	}

	for p, expected := range cases {
		_, err := ParsePath(p)
		valid := (err == nil)
		if valid != expected {
			t.Fatalf("expected %s to have valid == %t", p, expected)
		}
	}
}

func TestIsJustAKey(t *testing.T) {
	cases := map[string]bool{
		"QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n":           true,
		"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n":     true,
		"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a":   false,
		"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b": false,
		"/ipns/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n":     false,
	}

	for p, expected := range cases {
		path, err := ParsePath(p)
		if err != nil {
			t.Fatalf("ParsePath failed to parse \"%s\", but should have succeeded", p)
		}
		result := path.IsJustAKey()
		if result != expected {
			t.Fatalf("expected IsJustAKey(%s) to return %v, not %v", p, expected, result)
		}
	}
}

func TestPopLastSegment(t *testing.T) {
	cases := map[string][]string{
		"QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n":             []string{"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", ""},
		"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n":       []string{"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", ""},
		"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a":     []string{"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", "a"},
		"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b":   []string{"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a", "b"},
		"/ipns/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/x/y/z": []string{"/ipns/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/x/y", "z"},
	}

	for p, expected := range cases {
		path, err := ParsePath(p)
		if err != nil {
			t.Fatalf("ParsePath failed to parse \"%s\", but should have succeeded", p)
		}
		head, tail, err := path.PopLastSegment()
		if err != nil {
			t.Fatalf("PopLastSegment failed, but should have succeeded: %s", err)
		}
		headStr := head.String()
		if headStr != expected[0] {
			t.Fatalf("expected head of PopLastSegment(%s) to return %v, not %v", p, expected[0], headStr)
		}
		if tail != expected[1] {
			t.Fatalf("expected tail of PopLastSegment(%s) to return %v, not %v", p, expected[1], tail)
		}
	}
}
