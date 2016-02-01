package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"unicode/utf8"

	"github.com/attic-labs/noms/clients/csv"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
)

var (
	p       = flag.Uint("p", 512, "parallelism")
	dsFlags = dataset.NewFlags()
	// Actually the delimiter uses runes, which can be multiple characters long.
	// https://blog.golang.org/strings
	delimiter = flag.String("delimiter", ",", "field delimiter for csv file, must be exactly one character long.")
	header    = flag.String("header", "", "header row. If empty, we'll use the first row of the file")
	name      = flag.String("name", "Row", "struct name. The user-visible name to give to the struct type that will hold each row of data.")
)

func main() {
	cpuCount := runtime.NumCPU()
	runtime.GOMAXPROCS(cpuCount)

	flag.Usage = func() {
		fmt.Println("Usage: csv_importer [options] file\n")
		flag.PrintDefaults()
	}

	flag.Parse()
	ds := dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		return
	}
	defer ds.Store().Close()

	if flag.NArg() != 1 {
		fmt.Printf("Expected exactly one parameter (path) after flags, but you have %d. Maybe you put a flag after the path?\n", flag.NArg())
		flag.Usage()
		return
	}

	path := flag.Arg(0)
	if ds == nil || path == "" {
		flag.Usage()
		return
	}

	res, err := os.Open(path)
	d.Exp.NoError(err)
	defer res.Close()

	comma, err := getDelimiter(*delimiter)
	if err != nil {
		fmt.Println(err.Error())
		flag.Usage()
		return
	}

	value, _, _ := csv.Read(res, *name, *header, comma, *p, ds.Store())
	_, err = ds.Commit(value)
	d.Exp.NoError(err)
}

// Returns the rune contained in delimiter or an error.
func getDelimiter(delimiter string) (rune, error) {
	dlimLen := len(delimiter)
	if dlimLen == 0 {
		return 0, fmt.Errorf("delimiter flag must contain exactly one character (rune), not an empty string")
	}

	d, runeSize := utf8.DecodeRuneInString(delimiter)
	if d == utf8.RuneError {
		return 0, fmt.Errorf("Invalid utf8 string in delimiter flag: %s", delimiter)
	}
	if dlimLen != runeSize {
		return 0, fmt.Errorf("delimiter flag is too long. It must contain exactly one character (rune), but instead it is: %s", delimiter)
	}
	return d, nil
}
