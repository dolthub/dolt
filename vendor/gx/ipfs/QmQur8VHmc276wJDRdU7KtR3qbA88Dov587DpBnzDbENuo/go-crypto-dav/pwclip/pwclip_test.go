package pwclip

import (
	"bytes"
	"encoding/hex"
	"testing"
)

var passwordTestKeys = [][]byte{
	nil,
	[]byte{},
	[]byte("secret key"),
}

var extra = "extra data"

var passwordTests = []struct {
	pwm *PWM
	pws []string
}{
	{
		&PWM{
			Charset: CharsetAlphaNumeric,
			Length:  32,
		},
		[]string{
			"i9zqO9jaReJNR29EeUcjpVXM0trkFOr5",
			"8aRyuJhRGXAQFSlyngeSHpoVwjvKEBD9",
			"TxuEPdxmb4ps3KMFFm7K9YpCsLn1Yati",
		},
	},
	{
		&PWM{
			URL:      "example.com",
			Username: "example@example.com",
			Charset:  CharsetAlphaNumeric,
			Length:   32,
		},
		[]string{
			"uKSFwxsSAMW2thHm713g6uGve6zqCuHV",
			"2b8CMXZJYVDGqEq8cjJLeI7Vd1tDiVbR",
			"f67RwXJIQ9DtCTUcXyUogCzjO43TaY4V",
		},
	},
	{
		&PWM{
			URL:      "example.com",
			Username: "example@example.com",
			Charset:  CharsetAlphaNumeric,
			Length:   48,
		},
		[]string{
			"uKSFwxsSAMW2thHm713g6uGve6zqCuHVNxwcRHCXsdmhVY1k",
			"2b8CMXZJYVDGqEq8cjJLeI7Vd1tDiVbRRcmwf9gkjWIhdhgW",
			"f67RwXJIQ9DtCTUcXyUogCzjO43TaY4VvonXCxjapWXiM9vW",
		},
	},
	{
		&PWM{
			URL:      "example.com",
			Username: "example@example.com",
			Charset:  CharsetAlphaNumeric,
			Prefix:   "foobar!",
			Length:   32,
		},
		[]string{
			"foobar!uKSFwxsSAMW2thHm713g6uGve",
			"foobar!2b8CMXZJYVDGqEq8cjJLeI7Vd",
			"foobar!f67RwXJIQ9DtCTUcXyUogCzjO",
		},
	},
	{
		&PWM{
			URL:      "example.com",
			Username: "example@example.com",
			Extra:    &extra,
			Charset:  CharsetAlphaNumeric,
			Length:   32,
		},
		[]string{
			"d9vFmjXDQ8fTeHQ36knCCXhEQyiZsFMe",
			"3UIXCUj9Lme0f17aNJI5sMBa6l0DzKi9",
			"U1CdUb3gMyl2hpmIXZYLTQjQp16Sg9oX",
		},
	},
	{
		&PWM{
			URL:      "example.com",
			Username: "example@example.com",
			Charset:  "αβγδεζηθικλμνξοπρστυφχψω",
			Length:   32,
		},
		[]string{
			"ρλψυαυφνεγγμξβεωχιαγφδλλθτηγθμδβ",
			"ηθγρηωυδεχδλνινρψπδβηψμβυζπθαβσθ",
			"μβχτσξγηδσχφκφιωγολιφχβονθωρονμσ",
		},
	},
	{
		&PWM{
			URL:      "example.com",
			Username: "example@example.com",
			Charset:  "0⌘1",
			Length:   32,
		},
		[]string{
			"⌘⌘⌘⌘0⌘10⌘⌘1101⌘⌘⌘11010110⌘⌘⌘001⌘",
			"0⌘1⌘01⌘0⌘00⌘010⌘⌘00⌘00⌘1⌘⌘10⌘0⌘1",
			"1⌘⌘001⌘1001010111111⌘110⌘10⌘1⌘10",
		},
	},
}

func TestPassword(t *testing.T) {
	for i, test := range passwordTests {
		for k, expected := range test.pws {
			actual := test.pwm.Password(passwordTestKeys[k])
			if actual != expected {
				t.Errorf("test %d.%d:\n\texpected: %#v\n\tactually: %#v", i, k, expected, actual)
			}
		}
	}
}

var keyTests = []struct {
	passphrase []byte
	keyhex     string
}{
	{nil, "cf4b3589438e51bfc0f942ca1f2b108d5a9e5a9238c15a2e76ab764484e636bd"},
	{[]byte{}, "cf4b3589438e51bfc0f942ca1f2b108d5a9e5a9238c15a2e76ab764484e636bd"},
	{[]byte{0}, "cf4b3589438e51bfc0f942ca1f2b108d5a9e5a9238c15a2e76ab764484e636bd"},
	{[]byte{1}, "cdbc42a4bf57aad0b0a4a86d3bb654e57bc356bd08d5de88a6548a3a031fc87e"},
	{[]byte{1, 0}, "cdbc42a4bf57aad0b0a4a86d3bb654e57bc356bd08d5de88a6548a3a031fc87e"},
	{[]byte{1, 0, 1}, "aac60e84780340d5e7065a27a7189e240f8777b1b7ac2144e9d9e4d93a599c53"},
	{[]byte{0, 1}, "a35b6569ec9ac21d16c43db825436e92b5a23b6288e17503664962f148e72101"},
	{[]byte{0, 0, 1}, "df71d36f29d13211d9f74e77828cdc1c83e41a3a5407bc231bdca2d1504b1544"},
	{[]byte("passphrase"), "40f2dacf5fdb770dc6e047f41883ff71ec3972aa7ac92d1792dd2909f2453324"},
	{[]byte("another passphrase"), "2aaf56826d42fdcbe6ae5653f50b10fc47748d8c2b3e36515bb01078c7c8f535"},
}

func TestKey(t *testing.T) {
	for i, test := range keyTests {
		actual, err := Key(test.passphrase)
		if err != nil {
			t.Fatalf("Key error: %q", err)
		}
		expected, err := hex.DecodeString(test.keyhex)
		if err != nil {
			t.Fatalf("hex.Decode error: %q", err)
		}
		if !bytes.Equal(actual, expected) {
			keyhex := hex.EncodeToString(actual)
			t.Errorf("test %d:\n\texpected: %#v\n\tactually: %#v", i, test.keyhex, keyhex)
		}
	}
}

func BenchmarkKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Key([]byte("passphrase"))
	}
}
