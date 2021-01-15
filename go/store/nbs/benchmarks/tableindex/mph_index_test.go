
package tableindex

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/robskie/chd"
)

const (
	n = 1000 * 1000
	k = 20

	load = 0.9
)

func BenchmarkCHDIndex(b *testing.B) {
	var m *chd.Map
	bld, bb := setupCHD()

	b.Run("build MPH table", func(b *testing.B) {
		m, _ = bld.Build()
	})

	//checkSet := make(map[int]struct{}, n)
	//for i := 0; i < n; i++ {
	//	key := bb[i*k:(i+1)*k]
	//	val := m.Get(key)
	//
	//	if _, ok := checkSet[val]; ok {
	//		panic(val)
	//	}
	//	checkSet[val] = struct{}{}
	//}
	fmt.Println(len(bb))
	fmt.Println(m.Size())
}

func setupCHD() (*chd.Builder, []byte) {
	bb := make([]byte, n*k)
	_, err := rand.Read(bb)
	if err != nil {
		panic(err)
	}

	opts := chd.NewBuildOptions()
	opts.LoadFactor = load

	bld := chd.NewBuilder(opts)
	for i := 0; i < n; i++ {
		bld.Add(bb[i*k:(i+1)*k])
	}

	return bld, bb
}