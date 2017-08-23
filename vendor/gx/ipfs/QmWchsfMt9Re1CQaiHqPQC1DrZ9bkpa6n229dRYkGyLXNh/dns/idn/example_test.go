package idn_test

import (
	"fmt"
	"gx/ipfs/QmWchsfMt9Re1CQaiHqPQC1DrZ9bkpa6n229dRYkGyLXNh/dns/idn"
)

func ExampleToPunycode() {
	name := "インターネット.テスト"
	fmt.Printf("%s -> %s", name, idn.ToPunycode(name))
	// Output: インターネット.テスト -> xn--eckucmux0ukc.xn--zckzah
}

func ExampleFromPunycode() {
	name := "xn--mgbaja8a1hpac.xn--mgbachtv"
	fmt.Printf("%s -> %s", name, idn.FromPunycode(name))
	// Output: xn--mgbaja8a1hpac.xn--mgbachtv -> الانترنت.اختبار
}
