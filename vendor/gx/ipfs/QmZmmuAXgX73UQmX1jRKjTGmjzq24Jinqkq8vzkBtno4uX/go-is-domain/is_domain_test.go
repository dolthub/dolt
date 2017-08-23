package isdomain

import "testing"

func TestBasic(t *testing.T) {
	cases := map[string]bool{
		"foo.bar.baz.com":  true,
		"foo.bar.baz":      false,
		"foo.bar.baz.com.": true,
		"com":              false, // yeah yeah...
		".":                false, // yeah yeah...
		"..":               false,
		".foo.com.":        false,
		".foo.com":         false,
		"fo o.com":         false,
		"example.com":      true,
		"fjdoisajfdiosafdsa8fd8saf8dsa8fdsafdsa-fd-sa-fd-saf-dsa.org":   true,
		"fjdoisajfdiosafdsa8fd8saf8dsa8fdsafdsa-fd-sa-fd-saf-dsa.bit":   true,
		"fjdoisajfdiosafdsa8fd8saf8dsa8fdsafdsa-fd-sa-fd-saf-dsa.onion": true,
		"a.b.c.d.e.f.g.h.i.j.k.l.museum":                                true,
		"a.b.c.d.e.f.g.h.i.j.k.l":                                       false,
	}

	for d, ok := range cases {
		if IsDomain(d) != ok {
			t.Errorf("Misclassification: %v should be %v", d, ok)
		}
	}
}
