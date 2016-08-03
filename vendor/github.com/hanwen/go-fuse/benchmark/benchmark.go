package benchmark

// Routines for benchmarking fuse.

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"regexp"
	"sort"
	"time"
)

func ReadLines(name string) []string {
	f, err := os.Open(name)
	if err != nil {
		log.Fatal("ReadLines: ", err)
	}
	defer f.Close()
	r := bufio.NewReader(f)

	l := []string{}
	for {
		line, _, err := r.ReadLine()
		if line == nil || err != nil {
			break
		}

		fn := string(line)
		l = append(l, fn)
	}
	if len(l) == 0 {
		log.Fatal("no files added")
	}
	log.Printf("Read %d file names", len(l))

	return l
}

// Used for benchmarking.  Returns milliseconds.
func BulkStat(parallelism int, files []string) float64 {
	todo := make(chan string, len(files))
	dts := make(chan time.Duration, parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			t := time.Now()
			for {
				fn := <-todo
				if fn == "" {
					break
				}

				_, err := os.Lstat(fn)
				if err != nil {
					log.Fatal("All stats should succeed:", err)
				}
			}
			dts <- time.Now().Sub(t)
		}()
	}

	for _, v := range files {
		todo <- v
	}
	close(todo)

	total := 0.0
	for i := 0; i < parallelism; i++ {
		total += float64(<-dts) / float64(time.Millisecond)
	}

	avg := total / float64(len(files))

	return avg
}

func AnalyzeBenchmarkRuns(label string, times []float64) {
	sorted := times
	sort.Float64s(sorted)

	tot := 0.0
	for _, v := range times {
		tot += v
	}
	n := float64(len(times))

	avg := tot / n
	variance := 0.0
	for _, v := range times {
		variance += (v - avg) * (v - avg)
	}
	variance /= n

	stddev := math.Sqrt(variance)

	median := sorted[len(times)/2]
	perc90 := sorted[int(n*0.9)]
	perc10 := sorted[int(n*0.1)]

	fmt.Printf(
		"%s: %d samples\n"+
			"avg %.3fms +/- %.0f%% "+
			"median %.3fms, 10%%tiles: [-%.0f%%, +%.0f%%]\n",
		label,
		len(times), avg, 100.0*2*stddev/avg,
		median, 100*(median-perc10)/median, 100*(perc90-median)/median)
}

func RunBulkStat(runs int, threads int, sleepTime time.Duration, files []string) (results []float64) {
	for j := 0; j < runs; j++ {
		result := BulkStat(threads, files)
		results = append(results, result)

		if j < runs-1 {
			fmt.Printf("Sleeping %d seconds\n", sleepTime)
			time.Sleep(sleepTime)
		}
	}
	return results
}

func CountCpus() int {
	var contents [10240]byte

	f, err := os.Open("/proc/stat")
	defer f.Close()
	if err != nil {
		return 1
	}
	n, _ := f.Read(contents[:])
	re, _ := regexp.Compile("\ncpu[0-9]")

	return len(re.FindAllString(string(contents[:n]), 100))
}
