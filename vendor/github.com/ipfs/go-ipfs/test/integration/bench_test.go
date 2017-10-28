package integrationtest

import (
	"testing"

	"github.com/ipfs/go-ipfs/thirdparty/unit"
	testutil "gx/ipfs/QmWRCn8vruNAzHx8i6SAXinuheRitKEGu8c7m26stKvsYx/go-testutil"
)

func benchmarkAddCat(numBytes int64, conf testutil.LatencyConfig, b *testing.B) {

	b.StopTimer()
	b.SetBytes(numBytes)
	data := RandomBytes(numBytes) // we don't want to measure the time it takes to generate this data
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		if err := DirectAddCat(data, conf); err != nil {
			b.Fatal(err)
		}
	}
}

var instant = testutil.LatencyConfig{}.AllInstantaneous()

func BenchmarkInstantaneousAddCat1KB(b *testing.B)   { benchmarkAddCat(1*unit.KB, instant, b) }
func BenchmarkInstantaneousAddCat1MB(b *testing.B)   { benchmarkAddCat(1*unit.MB, instant, b) }
func BenchmarkInstantaneousAddCat2MB(b *testing.B)   { benchmarkAddCat(2*unit.MB, instant, b) }
func BenchmarkInstantaneousAddCat4MB(b *testing.B)   { benchmarkAddCat(4*unit.MB, instant, b) }
func BenchmarkInstantaneousAddCat8MB(b *testing.B)   { benchmarkAddCat(8*unit.MB, instant, b) }
func BenchmarkInstantaneousAddCat16MB(b *testing.B)  { benchmarkAddCat(16*unit.MB, instant, b) }
func BenchmarkInstantaneousAddCat32MB(b *testing.B)  { benchmarkAddCat(32*unit.MB, instant, b) }
func BenchmarkInstantaneousAddCat64MB(b *testing.B)  { benchmarkAddCat(64*unit.MB, instant, b) }
func BenchmarkInstantaneousAddCat128MB(b *testing.B) { benchmarkAddCat(128*unit.MB, instant, b) }
func BenchmarkInstantaneousAddCat256MB(b *testing.B) { benchmarkAddCat(256*unit.MB, instant, b) }

var routing = testutil.LatencyConfig{}.RoutingSlow()

func BenchmarkRoutingSlowAddCat1MB(b *testing.B)   { benchmarkAddCat(1*unit.MB, routing, b) }
func BenchmarkRoutingSlowAddCat2MB(b *testing.B)   { benchmarkAddCat(2*unit.MB, routing, b) }
func BenchmarkRoutingSlowAddCat4MB(b *testing.B)   { benchmarkAddCat(4*unit.MB, routing, b) }
func BenchmarkRoutingSlowAddCat8MB(b *testing.B)   { benchmarkAddCat(8*unit.MB, routing, b) }
func BenchmarkRoutingSlowAddCat16MB(b *testing.B)  { benchmarkAddCat(16*unit.MB, routing, b) }
func BenchmarkRoutingSlowAddCat32MB(b *testing.B)  { benchmarkAddCat(32*unit.MB, routing, b) }
func BenchmarkRoutingSlowAddCat64MB(b *testing.B)  { benchmarkAddCat(64*unit.MB, routing, b) }
func BenchmarkRoutingSlowAddCat128MB(b *testing.B) { benchmarkAddCat(128*unit.MB, routing, b) }
func BenchmarkRoutingSlowAddCat256MB(b *testing.B) { benchmarkAddCat(256*unit.MB, routing, b) }
func BenchmarkRoutingSlowAddCat512MB(b *testing.B) { benchmarkAddCat(512*unit.MB, routing, b) }

var network = testutil.LatencyConfig{}.NetworkNYtoSF()

func BenchmarkNetworkSlowAddCat1MB(b *testing.B)   { benchmarkAddCat(1*unit.MB, network, b) }
func BenchmarkNetworkSlowAddCat2MB(b *testing.B)   { benchmarkAddCat(2*unit.MB, network, b) }
func BenchmarkNetworkSlowAddCat4MB(b *testing.B)   { benchmarkAddCat(4*unit.MB, network, b) }
func BenchmarkNetworkSlowAddCat8MB(b *testing.B)   { benchmarkAddCat(8*unit.MB, network, b) }
func BenchmarkNetworkSlowAddCat16MB(b *testing.B)  { benchmarkAddCat(16*unit.MB, network, b) }
func BenchmarkNetworkSlowAddCat32MB(b *testing.B)  { benchmarkAddCat(32*unit.MB, network, b) }
func BenchmarkNetworkSlowAddCat64MB(b *testing.B)  { benchmarkAddCat(64*unit.MB, network, b) }
func BenchmarkNetworkSlowAddCat128MB(b *testing.B) { benchmarkAddCat(128*unit.MB, network, b) }
func BenchmarkNetworkSlowAddCat256MB(b *testing.B) { benchmarkAddCat(256*unit.MB, network, b) }

var hdd = testutil.LatencyConfig{}.Blockstore7200RPM()

func BenchmarkBlockstoreSlowAddCat1MB(b *testing.B)   { benchmarkAddCat(1*unit.MB, hdd, b) }
func BenchmarkBlockstoreSlowAddCat2MB(b *testing.B)   { benchmarkAddCat(2*unit.MB, hdd, b) }
func BenchmarkBlockstoreSlowAddCat4MB(b *testing.B)   { benchmarkAddCat(4*unit.MB, hdd, b) }
func BenchmarkBlockstoreSlowAddCat8MB(b *testing.B)   { benchmarkAddCat(8*unit.MB, hdd, b) }
func BenchmarkBlockstoreSlowAddCat16MB(b *testing.B)  { benchmarkAddCat(16*unit.MB, hdd, b) }
func BenchmarkBlockstoreSlowAddCat32MB(b *testing.B)  { benchmarkAddCat(32*unit.MB, hdd, b) }
func BenchmarkBlockstoreSlowAddCat64MB(b *testing.B)  { benchmarkAddCat(64*unit.MB, hdd, b) }
func BenchmarkBlockstoreSlowAddCat128MB(b *testing.B) { benchmarkAddCat(128*unit.MB, hdd, b) }
func BenchmarkBlockstoreSlowAddCat256MB(b *testing.B) { benchmarkAddCat(256*unit.MB, hdd, b) }

var mixed = testutil.LatencyConfig{}.NetworkNYtoSF().BlockstoreSlowSSD2014().RoutingSlow()

func BenchmarkMixedAddCat1MBXX(b *testing.B) { benchmarkAddCat(1*unit.MB, mixed, b) }
func BenchmarkMixedAddCat2MBXX(b *testing.B) { benchmarkAddCat(2*unit.MB, mixed, b) }
func BenchmarkMixedAddCat4MBXX(b *testing.B) { benchmarkAddCat(4*unit.MB, mixed, b) }
func BenchmarkMixedAddCat8MBXX(b *testing.B) { benchmarkAddCat(8*unit.MB, mixed, b) }
func BenchmarkMixedAddCat16MBX(b *testing.B) { benchmarkAddCat(16*unit.MB, mixed, b) }
func BenchmarkMixedAddCat32MBX(b *testing.B) { benchmarkAddCat(32*unit.MB, mixed, b) }
func BenchmarkMixedAddCat64MBX(b *testing.B) { benchmarkAddCat(64*unit.MB, mixed, b) }
func BenchmarkMixedAddCat128MB(b *testing.B) { benchmarkAddCat(128*unit.MB, mixed, b) }
func BenchmarkMixedAddCat256MB(b *testing.B) { benchmarkAddCat(256*unit.MB, mixed, b) }
