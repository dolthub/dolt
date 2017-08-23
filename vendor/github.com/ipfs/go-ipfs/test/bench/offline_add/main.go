package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"testing"

	"github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-random"
	"github.com/ipfs/go-ipfs/repo/config"
	"github.com/ipfs/go-ipfs/thirdparty/unit"
)

func main() {
	if err := compareResults(); err != nil {
		log.Fatal(err)
	}
}

func compareResults() error {
	var amount unit.Information
	for amount = 10 * unit.MB; amount > 0; amount = amount * 2 {
		if results, err := benchmarkAdd(int64(amount)); err != nil { // TODO compare
			return err
		} else {
			log.Println(amount, "\t", results)
		}
	}
	return nil
}

func benchmarkAdd(amount int64) (*testing.BenchmarkResult, error) {
	results := testing.Benchmark(func(b *testing.B) {
		b.SetBytes(amount)
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			tmpDir, err := ioutil.TempDir("", "")
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(tmpDir)

			env := append(os.Environ(), fmt.Sprintf("%s=%s", config.EnvDir, path.Join(tmpDir, config.DefaultPathName)))
			setupCmd := func(cmd *exec.Cmd) {
				cmd.Env = env
			}

			cmd := exec.Command("ipfs", "init", "-b=1024")
			setupCmd(cmd)
			if err := cmd.Run(); err != nil {
				b.Fatal(err)
			}

			const seed = 1
			f, err := ioutil.TempFile("", "")
			if err != nil {
				b.Fatal(err)
			}
			defer os.Remove(f.Name())

			random.WritePseudoRandomBytes(amount, f, seed)
			if err := f.Close(); err != nil {
				b.Fatal(err)
			}

			b.StartTimer()
			cmd = exec.Command("ipfs", "add", f.Name())
			setupCmd(cmd)
			if err := cmd.Run(); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
		}
	})
	return &results, nil
}
