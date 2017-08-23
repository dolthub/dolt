package bbloom

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"testing"
)

var (
	wordlist1 [][]byte
	n         = 1 << 16
	bf        Bloom
)

func TestMain(m *testing.M) {
	file, err := os.Open("words.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	wordlist1 = make([][]byte, n)
	for i := range wordlist1 {
		if scanner.Scan() {
			wordlist1[i] = []byte(scanner.Text())
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("\n###############\nbbloom_test.go")
	fmt.Print("Benchmarks relate to 2**16 OP. --> output/65536 op/ns\n###############\n\n")

	os.Exit(m.Run())

}

func TestM_NumberOfWrongs(t *testing.T) {
	bf, err := New(float64(n*10), float64(7))
	if err != nil {
		t.Fatal(err)
	}

	cnt := 0
	for i := range wordlist1 {
		if !bf.AddIfNotHas(wordlist1[i]) {
			cnt++
		}
	}
	fmt.Printf("Bloomfilter New(7* 2**16, 7) (-> size=%v bit): \n            Check for 'false positives': %v wrong positive 'Has' results on 2**16 entries => %v %%\n", len(bf.bitset)<<6, cnt, float64(cnt)/float64(n))

}

func TestM_JSON(t *testing.T) {
	const shallBe = int(1 << 16)

	bf, err := New(float64(n*10), float64(7))
	if err != nil {
		t.Fatal(err)
	}

	cnt := 0
	for i := range wordlist1 {
		if !bf.AddIfNotHas(wordlist1[i]) {
			cnt++
		}
	}

	Json, err := bf.JSONMarshal()
	if err != nil {
		t.Fatal(err)
	}

	// create new bloomfilter from bloomfilter's JSON representation
	bf2 := JSONUnmarshal(Json)

	cnt2 := 0
	for i := range wordlist1 {
		if !bf2.AddIfNotHas(wordlist1[i]) {
			cnt2++
		}
	}

	if cnt2 != shallBe {
		t.Errorf("FAILED !AddIfNotHas = %v; want %v", cnt2, shallBe)
	}
}
func TestFillRatio(t *testing.T) {
	bf, err := New(float64(512), float64(7))
	if err != nil {
		t.Fatal(err)
	}
	bf.Add([]byte("test"))
	r := bf.FillRatio()
	if math.Abs(r-float64(7)/float64(512)) > 0.001 {
		t.Error("ratio doesn't work")
	}
}

func ExampleM_NewAddHasAddIfNotHas() {
	bf, err := New(float64(512), float64(1))
	if err != nil {
		panic(err)
	}

	fmt.Printf("%v %v %v %v\n", bf.sizeExp, bf.size, bf.setLocs, bf.shift)

	bf.Add([]byte("Manfred"))
	fmt.Println("bf.Add([]byte(\"Manfred\"))")
	fmt.Printf("bf.Has([]byte(\"Manfred\")) -> %v - should be true\n", bf.Has([]byte("Manfred")))
	fmt.Printf("bf.Add([]byte(\"manfred\")) -> %v - should be false\n", bf.Has([]byte("manfred")))
	fmt.Printf("bf.AddIfNotHas([]byte(\"Manfred\")) -> %v - should be false\n", bf.AddIfNotHas([]byte("Manfred")))
	fmt.Printf("bf.AddIfNotHas([]byte(\"manfred\")) -> %v - should be true\n", bf.AddIfNotHas([]byte("manfred")))

	bf.AddTS([]byte("Hans"))
	fmt.Println("bf.AddTS([]byte(\"Hans\")")
	fmt.Printf("bf.HasTS([]byte(\"Hans\")) -> %v - should be true\n", bf.HasTS([]byte("Hans")))
	fmt.Printf("bf.AddTS([]byte(\"hans\")) -> %v - should be false\n", bf.HasTS([]byte("hans")))
	fmt.Printf("bf.AddIfNotHasTS([]byte(\"Hans\")) -> %v - should be false\n", bf.AddIfNotHasTS([]byte("Hans")))
	fmt.Printf("bf.AddIfNotHasTS([]byte(\"hans\")) -> %v - should be true\n", bf.AddIfNotHasTS([]byte("hans")))

	// Output: 9 511 1 55
	// bf.Add([]byte("Manfred"))
	// bf.Has([]byte("Manfred")) -> true - should be true
	// bf.Add([]byte("manfred")) -> false - should be false
	// bf.AddIfNotHas([]byte("Manfred")) -> false - should be false
	// bf.AddIfNotHas([]byte("manfred")) -> true - should be true
	// bf.AddTS([]byte("Hans")
	// bf.HasTS([]byte("Hans")) -> true - should be true
	// bf.AddTS([]byte("hans")) -> false - should be false
	// bf.AddIfNotHasTS([]byte("Hans")) -> false - should be false
	// bf.AddIfNotHasTS([]byte("hans")) -> true - should be true
}

func BenchmarkM_New(b *testing.B) {
	for r := 0; r < b.N; r++ {
		_, _ = New(float64(n*10), float64(7))
	}
}

func BenchmarkM_Clear(b *testing.B) {
	bf, err := New(float64(n*10), float64(7))
	if err != nil {
		b.Fatal(err)
	}
	for i := range wordlist1 {
		bf.Add(wordlist1[i])
	}
	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		bf.Clear()
	}
}

func BenchmarkM_Add(b *testing.B) {
	bf, err := New(float64(n*10), float64(7))
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		for i := range wordlist1 {
			bf.Add(wordlist1[i])
		}
	}

}

func BenchmarkM_Has(b *testing.B) {
	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		for i := range wordlist1 {
			bf.Has(wordlist1[i])
		}
	}

}

func BenchmarkM_AddIfNotHasFALSE(b *testing.B) {
	bf, err := New(float64(n*10), float64(7))
	if err != nil {
		b.Fatal(err)
	}
	for i := range wordlist1 {
		bf.Has(wordlist1[i])
	}
	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		for i := range wordlist1 {
			bf.AddIfNotHas(wordlist1[i])
		}
	}
}

func BenchmarkM_AddIfNotHasClearTRUE(b *testing.B) {
	bf, err := New(float64(n*10), float64(7))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		for i := range wordlist1 {
			bf.AddIfNotHas(wordlist1[i])
		}
		bf.Clear()
	}
}

func BenchmarkM_AddTS(b *testing.B) {
	bf, err := New(float64(n*10), float64(7))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		for i := range wordlist1 {
			bf.AddTS(wordlist1[i])
		}
	}

}

func BenchmarkM_HasTS(b *testing.B) {
	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		for i := range wordlist1 {
			bf.HasTS(wordlist1[i])
		}
	}

}

func BenchmarkM_AddIfNotHasTSFALSE(b *testing.B) {
	bf, err := New(float64(n*10), float64(7))
	if err != nil {
		b.Fatal(err)
	}
	for i := range wordlist1 {
		bf.Has(wordlist1[i])
	}
	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		for i := range wordlist1 {
			bf.AddIfNotHasTS(wordlist1[i])
		}
	}
}

func BenchmarkM_AddIfNotHasTSClearTRUE(b *testing.B) {
	bf, err := New(float64(n*10), float64(7))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		for i := range wordlist1 {
			bf.AddIfNotHasTS(wordlist1[i])
		}
		bf.Clear()
	}

}
