package crypto

import "testing"

func BenchmarkSignRSA1B(b *testing.B)      { RunBenchmarkSignRSA(b, 1) }
func BenchmarkSignRSA10B(b *testing.B)     { RunBenchmarkSignRSA(b, 10) }
func BenchmarkSignRSA100B(b *testing.B)    { RunBenchmarkSignRSA(b, 100) }
func BenchmarkSignRSA1000B(b *testing.B)   { RunBenchmarkSignRSA(b, 1000) }
func BenchmarkSignRSA10000B(b *testing.B)  { RunBenchmarkSignRSA(b, 10000) }
func BenchmarkSignRSA100000B(b *testing.B) { RunBenchmarkSignRSA(b, 100000) }

func BenchmarkVerifyRSA1B(b *testing.B)      { RunBenchmarkVerifyRSA(b, 1) }
func BenchmarkVerifyRSA10B(b *testing.B)     { RunBenchmarkVerifyRSA(b, 10) }
func BenchmarkVerifyRSA100B(b *testing.B)    { RunBenchmarkVerifyRSA(b, 100) }
func BenchmarkVerifyRSA1000B(b *testing.B)   { RunBenchmarkVerifyRSA(b, 1000) }
func BenchmarkVerifyRSA10000B(b *testing.B)  { RunBenchmarkVerifyRSA(b, 10000) }
func BenchmarkVerifyRSA100000B(b *testing.B) { RunBenchmarkVerifyRSA(b, 100000) }

func BenchmarkSignEd255191B(b *testing.B)      { RunBenchmarkSignEd25519(b, 1) }
func BenchmarkSignEd2551910B(b *testing.B)     { RunBenchmarkSignEd25519(b, 10) }
func BenchmarkSignEd25519100B(b *testing.B)    { RunBenchmarkSignEd25519(b, 100) }
func BenchmarkSignEd255191000B(b *testing.B)   { RunBenchmarkSignEd25519(b, 1000) }
func BenchmarkSignEd2551910000B(b *testing.B)  { RunBenchmarkSignEd25519(b, 10000) }
func BenchmarkSignEd25519100000B(b *testing.B) { RunBenchmarkSignEd25519(b, 100000) }

func BenchmarkVerifyEd255191B(b *testing.B)      { RunBenchmarkVerifyEd25519(b, 1) }
func BenchmarkVerifyEd2551910B(b *testing.B)     { RunBenchmarkVerifyEd25519(b, 10) }
func BenchmarkVerifyEd25519100B(b *testing.B)    { RunBenchmarkVerifyEd25519(b, 100) }
func BenchmarkVerifyEd255191000B(b *testing.B)   { RunBenchmarkVerifyEd25519(b, 1000) }
func BenchmarkVerifyEd2551910000B(b *testing.B)  { RunBenchmarkVerifyEd25519(b, 10000) }
func BenchmarkVerifyEd25519100000B(b *testing.B) { RunBenchmarkVerifyEd25519(b, 100000) }

func RunBenchmarkSignRSA(b *testing.B, numBytes int) {
	runBenchmarkSign(b, numBytes, RSA)
}

func RunBenchmarkSignEd25519(b *testing.B, numBytes int) {
	runBenchmarkSign(b, numBytes, Ed25519)
}

func runBenchmarkSign(b *testing.B, numBytes int, t int) {
	secret, _, err := GenerateKeyPair(t, 1024)
	if err != nil {
		b.Fatal(err)
	}
	someData := make([]byte, numBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := secret.Sign(someData)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func RunBenchmarkVerifyRSA(b *testing.B, numBytes int) {
	runBenchmarkSign(b, numBytes, RSA)
}

func RunBenchmarkVerifyEd25519(b *testing.B, numBytes int) {
	runBenchmarkSign(b, numBytes, Ed25519)
}

func runBenchmarkVerify(b *testing.B, numBytes int, t int) {
	secret, public, err := GenerateKeyPair(t, 1024)
	if err != nil {
		b.Fatal(err)
	}
	someData := make([]byte, numBytes)
	signature, err := secret.Sign(someData)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		valid, err := public.Verify(someData, signature)
		if err != nil {
			b.Fatal(err)
		}
		if !valid {
			b.Fatal("signature should be valid")
		}
	}
}
