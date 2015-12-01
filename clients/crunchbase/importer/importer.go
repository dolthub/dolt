package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"strings"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/tealeg/xlsx"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

var (
	date1904 = false
	dsFlags  = dataset.NewFlags()
)

func main() {
	var ds *dataset.Dataset

	flag.Usage = func() {
		fmt.Println("Usage: crunchbase [options] url\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	httpClient := util.CachingHttpClient()
	ds = dsFlags.CreateDataset()
	url := flag.Arg(0)
	if httpClient == nil || ds == nil || url == "" {
		flag.Usage()
		return
	}
	defer ds.Close()

	fmt.Print("Fetching excel file - this can take a minute or so...")
	resp, err := httpClient.Get(url)
	d.Exp.NoError(err)
	defer resp.Body.Close()

	tempFile, err := ioutil.TempFile(os.TempDir(), "")
	defer tempFile.Close()
	d.Chk.NoError(err)

	h := sha1.New()
	_, err = io.Copy(io.MultiWriter(h, tempFile), resp.Body)
	d.Chk.NoError(err)

	companiesRef := getExistingCompaniesRef(*ds, h)
	if !companiesRef.IsEmpty() {
		fmt.Println("\rExcel file hasn't changed since last run, nothing to do.")
	} else {
		companiesRef = importCompanies(*ds, tempFile.Name())
	}

	// Commit ref of the companiesRef list
	_, ok := ds.Commit(ImportDef{
		ref.FromHash(h).String(),
		DateDef{time.Now().Format(time.RFC3339)},
		companiesRef,
	}.New(ds.Store()))
	d.Exp.True(ok, "Could not commit due to conflicting edit")
}

func importCompanies(ds dataset.Dataset, fileName string) ref.Ref {
	fmt.Print("\rOpening excel file - this can take a minute or so...")

	xlFile, err := xlsx.OpenFile(fileName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(-1)
	}

	fmt.Print("\rImporting...")

	date1904 = xlFile.Date1904

	// Read in Rounds and group  according to CompanyPermalink
	roundsByPermalink := map[string][]Round{}
	roundsSheet := xlFile.Sheet["Rounds"]
	numRounds := 0
	for i, row := range roundsSheet.Rows {
		if i != 0 {
			round := NewRoundFromRow(ds.Store(), row)
			pl := round.CompanyPermalink()
			roundsByPermalink[pl] = append(roundsByPermalink[pl], round)
			numRounds++
		}
	}

	// Read in Companies and map to permalink
	companyRefs := NewMapOfStringToRefOfCompany(ds.Store())
	companySheet := xlFile.Sheet["Companies"]
	for i, row := range companySheet.Rows {
		fmt.Printf("\rImporting %d of %d rounds... (%.2f%%)", i, len(companySheet.Rows), float64(i)/float64(len(companySheet.Rows))*float64(100))
		if i != 0 {
			company := NewCompanyFromRow(ds.Store(), row)
			permalink := company.Permalink()

			rounds := roundsByPermalink[permalink]
			roundRefs := NewSetOfRefOfRound(ds.Store())
			for _, r := range rounds {
				ref := types.WriteValue(r, ds.Store())
				roundRefs = roundRefs.Insert(NewRefOfRound(ref))
			}
			company = company.SetRounds(roundRefs)
			ref := types.WriteValue(company, ds.Store())
			refOfCompany := NewRefOfCompany(ref)
			companyRefs = companyRefs.Set(company.Permalink(), refOfCompany)
		}
	}

	fmt.Printf("\r### imported %d companies with %d rounds\n", companyRefs.Len(), numRounds)

	// Write the list of companyRefs
	return types.WriteValue(companyRefs, ds.Store())
}

func getExistingCompaniesRef(ds dataset.Dataset, h hash.Hash) ref.Ref {
	if head, ok := ds.MaybeHead(); ok {
		if imp, ok := head.Value().(Import); ok {
			if imp.FileSHA1() == ref.FromHash(h).String() {
				return imp.Companies().TargetRef()
			}
		}
	}
	return ref.Ref{}
}

func NewCompanyFromRow(cs chunks.ChunkStore, row *xlsx.Row) Company {
	cells := row.Cells

	company := CompanyDef{
		Permalink:       cells[0].Value,
		Name:            cells[1].Value,
		HomepageUrl:     cells[2].Value,
		CategoryList:    parseListOfCategory(cells[3].Value),
		Market:          cells[4].Value,
		FundingTotalUsd: parseFloatValue(cells[5], "Company.FundingTotalUsd"),
		Status:          cells[6].Value,
		CountryCode:     cells[7].Value,
		StateCode:       cells[8].Value,
		Region:          cells[9].Value,
		City:            cells[10].Value,
		FundingRounds:   uint16(parseIntValue(cells[11], "Company.FundingRounds")),
		FoundedAt:       parseTimeStamp(cells[12], "Company.FoundedAt"),
		// Skip FoundedMonth: 13
		// Skip FoundedYear:  14
		FirstFundingAt: parseTimeStamp(cells[15], "Company.FirstFundingAt"),
		LastFundingAt:  parseTimeStamp(cells[16], "Company.LastFundingAt"),
	}
	return company.New(cs)
}

func NewRoundFromRow(cs chunks.ChunkStore, row *xlsx.Row) Round {
	cells := row.Cells

	var raisedAmountUsd float64
	if len(cells) < 16 {
		fmt.Printf("warning: Found Round with only %d cells - expected 16!\n", len(cells))
		raisedAmountUsd = 0
	} else {
		raisedAmountUsd = parseFloatValue(cells[15], "Round.raisedAmountUsd")
	}

	round := RoundDef{
		CompanyPermalink:      cells[0].Value,
		FundingRoundPermalink: cells[8].Value,
		FundingRoundType:      cells[9].Value,
		FundingRoundCode:      cells[10].Value,
		FundedAt:              parseTimeStamp(cells[11], "Round.fundedAt"),
		// Skip FundedMonth:   12
		// Skip FundedQuarter: 13
		// Skip FundedYear:    14
		RaisedAmountUsd: raisedAmountUsd,
	}
	return round.New(cs)
}

func parseListOfCategory(s string) SetOfStringDef {
	elems := strings.Split(s, "|")
	realElems := SetOfStringDef{}
	for _, elem := range elems {
		s1 := strings.TrimSpace(elem)
		if s1 != "" {
			realElems[s1] = true
		}
	}
	return realElems
}

func parseFloatValue(cell *xlsx.Cell, field string) float64 {
	v := strings.TrimSpace(cell.Value)
	parsedValue := float64(0)
	if v != "" {
		var err error
		parsedValue, err = cell.Float()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Parse failure on field: %s, err: %s\n", field, err)
		}
	}
	return float64(parsedValue)
}

func parseIntValue(cell *xlsx.Cell, field string) int {
	v := strings.TrimSpace(cell.Value)
	parsedValue := 0
	if v != "" {
		var err error
		parsedValue, err = cell.Int()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Parse failure on field: %s, err: %s\n", field, err)
		}
	}
	return int(parsedValue)
}

func parseTimeStamp(cell *xlsx.Cell, field string) int64 {
	if f, err := strconv.ParseFloat(cell.Value, 64); err == nil {
		return xlsx.TimeFromExcelTime(f, date1904).Unix()
	}
	const shortForm = "2006-01-02"
	if t, err := time.Parse(shortForm, cell.Value); err == nil {
		return t.Unix()
	}
	if cell.Value != "" {
		fmt.Fprintf(os.Stderr, "Could not parse field as date: %s, \"%s\"\n", field, cell.Value)
	}
	return 0
}
