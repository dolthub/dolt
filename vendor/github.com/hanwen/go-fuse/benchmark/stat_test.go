package benchmark

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

func setupFs(fs pathfs.FileSystem) (string, func()) {
	opts := &nodefs.Options{
		EntryTimeout:    0.0,
		AttrTimeout:     0.0,
		NegativeTimeout: 0.0,
	}
	mountPoint, _ := ioutil.TempDir("", "stat_test")
	nfs := pathfs.NewPathNodeFs(fs, nil)
	state, _, err := nodefs.MountRoot(mountPoint, nfs.Root(), opts)
	if err != nil {
		panic(fmt.Sprintf("cannot mount %v", err)) // ugh - benchmark has no error methods.
	}
	lmap := NewLatencyMap()

	state.RecordLatencies(lmap)
	go state.Serve()

	return mountPoint, func() {
		lc, lns := lmap.Get("LOOKUP")
		gc, gns := lmap.Get("GETATTR")
		fmt.Printf("GETATTR %v/call n=%d, LOOKUP %v/call n=%d\n",
			gns/time.Duration(gc), gc,
			lns/time.Duration(lc), lc)

		err := state.Unmount()
		if err != nil {
			log.Println("error during unmount", err)
		} else {
			os.RemoveAll(mountPoint)
		}
	}
}

func TestNewStatFs(t *testing.T) {
	fs := NewStatFs()
	for _, n := range []string{
		"file.txt", "sub/dir/foo.txt",
		"sub/dir/bar.txt", "sub/marine.txt"} {
		fs.AddFile(n)
	}

	wd, clean := setupFs(fs)
	defer clean()

	names, err := ioutil.ReadDir(wd)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	if len(names) != 2 {
		t.Error("readdir /", names)
	}

	fi, err := os.Lstat(wd + "/sub")
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	if !fi.IsDir() {
		t.Error("mode", fi)
	}
	names, err = ioutil.ReadDir(wd + "/sub")
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	if len(names) != 2 {
		t.Error("readdir /sub", names)
	}
	names, err = ioutil.ReadDir(wd + "/sub/dir")
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	if len(names) != 2 {
		t.Error("readdir /sub/dir", names)
	}

	fi, err = os.Lstat(wd + "/sub/marine.txt")
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	if fi.Mode()&os.ModeType != 0 {
		t.Error("mode", fi)
	}
}

func BenchmarkGoFuseThreadedStat(b *testing.B) {
	b.StopTimer()
	fs := NewStatFs()
	fs.delay = delay

	wd, _ := os.Getwd()
	files := ReadLines(wd + "/testpaths.txt")
	for _, fn := range files {
		fs.AddFile(fn)
	}

	wd, clean := setupFs(fs)
	defer clean()

	for i, l := range files {
		files[i] = filepath.Join(wd, l)
	}

	threads := runtime.GOMAXPROCS(0)
	results := TestingBOnePass(b, threads, files)
	AnalyzeBenchmarkRuns(fmt.Sprintf("Go-FUSE %d CPUs", threads), results)
}

func TestingBOnePass(b *testing.B, threads int, files []string) (results []float64) {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	todo := b.N
	for todo > 0 {
		if len(files) > todo {
			files = files[:todo]
		}
		b.StartTimer()
		result := BulkStat(threads, files)
		todo -= len(files)
		b.StopTimer()
		results = append(results, result)
	}
	runtime.ReadMemStats(&after)

	fmt.Printf("GC count %d, total GC time: %d ns/file\n",
		after.NumGC-before.NumGC, (after.PauseTotalNs-before.PauseTotalNs)/uint64(b.N))
	return results
}

// Add this so we can estimate impact on latency numbers.
func BenchmarkTimeNow(b *testing.B) {
	for i := 0; i < b.N; i++ {
		time.Now()
	}
}

func BenchmarkCFuseThreadedStat(b *testing.B) {
	b.StopTimer()

	wd, _ := os.Getwd()
	lines := ReadLines(wd + "/testpaths.txt")
	unique := map[string]int{}
	for _, l := range lines {
		unique[l] = 1
		dir, _ := filepath.Split(l)
		for dir != "/" && dir != "" {
			unique[dir] = 1
			dir = filepath.Clean(dir)
			dir, _ = filepath.Split(dir)
		}
	}

	out := []string{}
	for k := range unique {
		out = append(out, k)
	}

	f, err := ioutil.TempFile("", "")
	if err != nil {
		b.Fatalf("failed: %v", err)
	}
	sort.Strings(out)
	for _, k := range out {
		f.Write([]byte(fmt.Sprintf("/%s\n", k)))
	}
	f.Close()

	mountPoint, _ := ioutil.TempDir("", "stat_test")
	cmd := exec.Command(wd+"/cstatfs",
		"-o",
		"entry_timeout=0.0,attr_timeout=0.0,ac_attr_timeout=0.0,negative_timeout=0.0",
		mountPoint)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("STATFS_INPUT=%s", f.Name()),
		fmt.Sprintf("STATFS_DELAY_USEC=%d", delay/time.Microsecond))
	cmd.Start()

	bin, err := exec.LookPath("fusermount")
	if err != nil {
		b.Fatalf("failed: %v", err)
	}
	stop := exec.Command(bin, "-u", mountPoint)
	if err != nil {
		b.Fatalf("failed: %v", err)
	}
	defer stop.Run()

	for i, l := range lines {
		lines[i] = filepath.Join(mountPoint, l)
	}

	time.Sleep(100 * time.Millisecond)
	os.Lstat(mountPoint)
	threads := runtime.GOMAXPROCS(0)
	results := TestingBOnePass(b, threads, lines)
	AnalyzeBenchmarkRuns(fmt.Sprintf("CFuse on %d CPUS", threads), results)
}
