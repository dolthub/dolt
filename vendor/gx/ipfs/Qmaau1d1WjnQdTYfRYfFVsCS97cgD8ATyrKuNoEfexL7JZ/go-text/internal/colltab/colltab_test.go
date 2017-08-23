package colltab

import (
	"testing"

	"gx/ipfs/Qmaau1d1WjnQdTYfRYfFVsCS97cgD8ATyrKuNoEfexL7JZ/go-text/language"
)

func TestMatchLang(t *testing.T) {
	tags := []language.Tag{
		0:  language.Und,
		1:  language.MustParse("bs"),
		2:  language.German,
		3:  language.English,
		4:  language.AmericanEnglish,
		5:  language.MustParse("en-US-u-va-posix"),
		6:  language.Portuguese,
		7:  language.Serbian,
		8:  language.MustParse("sr-Latn"),
		9:  language.Chinese,
		10: language.SimplifiedChinese, // Cannot match.
		11: language.TraditionalChinese,
	}
	for i, tc := range []struct {
		x int
		t language.Tag
	}{
		{0, language.Und},
		{0, language.Persian}, // Default to first element when no match.
		{3, language.English},
		{4, language.AmericanEnglish},
		{5, language.MustParse("en-US-u-va-posix")},   // Ext. variant match.
		{4, language.MustParse("en-US-u-va-noposix")}, // Ext. variant mismatch.
		{3, language.MustParse("en-UK-u-va-noposix")}, // Ext. variant mismatch.
		{7, language.Serbian},
		{0, language.Croatian},             // Don't match to close language!
		{0, language.MustParse("gsw")},     // Don't match to close language!
		{1, language.MustParse("bs-Cyrl")}, // Odd, but correct.
		{1, language.MustParse("bs-Latn")}, // Estimated script drops.
		{8, language.MustParse("sr-Latn")},
		{9, language.Chinese},
		{10, language.SimplifiedChinese}, // Default script drops.
		{11, language.TraditionalChinese},
		{11, language.MustParse("und-TW")},     // Infer script and language.
		{11, language.MustParse("und-HK")},     // Infer script and language.
		{6, language.MustParse("und-BR")},      // Infer script and language.
		{6, language.MustParse("und-PT")},      // Infer script and language.
		{2, language.MustParse("und-Latn-DE")}, // Infer language.
		{0, language.MustParse("und-Jpan-BR")}, // Infers "ja", so no match.
		{0, language.MustParse("zu")},          // No match past index.
	} {
		if x := MatchLang(tc.t, tags); x != tc.x {
			t.Errorf("%d: MatchLang(%q, tags) = %d; want %d", i, tc.t, x, tc.x)
		}
	}
}
