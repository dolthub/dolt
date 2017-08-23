package currency

import (
	"flag"
	"strings"
	"testing"

	"gx/ipfs/Qmaau1d1WjnQdTYfRYfFVsCS97cgD8ATyrKuNoEfexL7JZ/go-text/internal/gen"
	"gx/ipfs/Qmaau1d1WjnQdTYfRYfFVsCS97cgD8ATyrKuNoEfexL7JZ/go-text/language"
	"gx/ipfs/Qmaau1d1WjnQdTYfRYfFVsCS97cgD8ATyrKuNoEfexL7JZ/go-text/message"
	"gx/ipfs/Qmaau1d1WjnQdTYfRYfFVsCS97cgD8ATyrKuNoEfexL7JZ/go-text/unicode/cldr"
)

var (
	long = flag.Bool("long", false,
		"run time-consuming tests, such as tests that fetch data online")

	draft = flag.String("draft",
		"contributed",
		`Minimal draft requirements (approved, contributed, provisional, unconfirmed).`)
)

func TestTables(t *testing.T) {
	if !*long {
		return
	}

	gen.Init()

	// Read the CLDR zip file.
	r := gen.OpenCLDRCoreZip()
	defer r.Close()

	d := &cldr.Decoder{}
	d.SetDirFilter("supplemental", "main")
	d.SetSectionFilter("numbers")
	data, err := d.DecodeZip(r)
	if err != nil {
		t.Fatalf("DecodeZip: %v", err)
	}

	dr, err := cldr.ParseDraft(*draft)
	if err != nil {
		t.Fatalf("filter: %v", err)
	}

	for _, lang := range data.Locales() {
		p := message.NewPrinter(language.MustParse(lang))

		ldml := data.RawLDML(lang)
		if ldml.Numbers == nil || ldml.Numbers.Currencies == nil {
			continue
		}
		for _, c := range ldml.Numbers.Currencies.Currency {
			syms := cldr.MakeSlice(&c.Symbol)
			syms.SelectDraft(dr)

			for _, sym := range c.Symbol {
				cur, err := ParseISO(c.Type)
				if err != nil {
					continue
				}
				formatter := Symbol
				switch sym.Alt {
				case "":
				case "narrow":
					formatter = NarrowSymbol
				default:
					continue
				}
				want := sym.Data()
				if got := p.Sprint(formatter(cur)); got != want {
					t.Errorf("%s:%sSymbol(%s) = %s; want %s", lang, strings.Title(sym.Alt), c.Type, got, want)
				}
			}
		}
	}
}
