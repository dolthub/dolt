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

	imp := ImportDef{
		ref.FromHash(h).String(),
		DateDef{unixMs(time.Now())},
		companiesRef,
	}.New(ds.Store())

	// Commit ref of the companiesRef list
	_, err = ds.Commit(imp)
	d.Exp.NoError(err)
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
	roundsColIndexes := readIndexesFromHeaderRow(roundsSheet)
	numRounds := 0
	for i, row := range roundsSheet.Rows {
		if i != 0 {
			round := newRoundFromRow(ds.Store(), roundsColIndexes, row, i)
			pl := round.CompanyPermalink()
			roundsByPermalink[pl] = append(roundsByPermalink[pl], round)
			numRounds++
		}
	}

	// Read in Companies and map to permalink
	companyRefsDef := MapOfStringToRefOfCompanyDef{}
	companySheet := xlFile.Sheet["Companies"]
	companyColIndexes := readIndexesFromHeaderRow(companySheet)
	for i, row := range companySheet.Rows {
		fmt.Printf("\rImporting %d of %d companies... (%.2f%%)", i, len(companySheet.Rows), float64(i)/float64(len(companySheet.Rows))*float64(100))
		if i != 0 {
			company := newCompanyFromRow(ds.Store(), companyColIndexes, row, i)
			permalink := company.Permalink()

			rounds := roundsByPermalink[permalink]
			roundRefs := SetOfRefOfRoundDef{}
			for _, r := range rounds {
				ref := types.WriteValue(r, ds.Store())
				roundRefs[ref] = true
			}
			company = company.SetRounds(roundRefs.New(ds.Store()))
			ref := types.WriteValue(company, ds.Store())
			companyRefsDef[company.Permalink()] = ref
		}
	}

	companyRefs := companyRefsDef.New(ds.Store())

	// Uncomment this line of code once Len() is implemented on compoundLists
	//	fmt.Printf("\rImported %d companies with %d rounds\n", companyRefs.Len(), numRounds)

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

func newCompanyFromRow(cs chunks.ChunkStore, idxs columnIndexes, row *xlsx.Row, rowNum int) Company {
	cells := row.Cells

	company := CompanyDef{
		Permalink:       idxs.getString("permalink", cells),
		Name:            idxs.getString("name", cells),
		HomepageUrl:     idxs.getString("homepage_url", cells),
		CategoryList:    idxs.getListOfCategory("category_list", cells),
		Market:          idxs.getString("market", cells),
		FundingTotalUsd: idxs.getFloat("fundingTotalUsd", cells, "Company.FundingTotalUsd", rowNum),
		Status:          idxs.getString("status", cells),
		CountryCode:     idxs.getString("country_code", cells),
		StateCode:       idxs.getString("state_code", cells),
		Region:          idxs.getString("region", cells),
		City:            idxs.getString("city", cells),
		FundingRounds:   uint16(idxs.getInt("funding_rounds", cells, "Company.FundingRounds", rowNum)),
		FoundedAt:       idxs.getDate("founded_at", cells, "Company.FoundedAt", rowNum),
		FirstFundingAt:  idxs.getDate("first_funding_at", cells, "Company.FirstFundingAt", rowNum),
		LastFundingAt:   idxs.getDate("last_funding_at", cells, "Company.LastFundingAt", rowNum),
	}
	return company.New(cs)
}

func newRoundFromRow(cs chunks.ChunkStore, idxs columnIndexes, row *xlsx.Row, rowNum int) Round {
	cells := row.Cells

	round := RoundDef{
		CompanyPermalink:      idxs.getString("company_permalink", cells),
		FundingRoundPermalink: idxs.getString("funding_round_permalink", cells),
		FundingRoundType:      idxs.getString("funding_round_type", cells),
		FundingRoundCode:      idxs.getString("funding_round_code", cells),
		FundedAt:              idxs.getDate("funded_at", cells, "Round.fundedAt", rowNum),
		RaisedAmountUsd:       idxs.getFloat("raised_amount_usd", cells, "Round.raisedAmountUsd", rowNum),
	}
	return round.New(cs)
}

type columnIndexes map[string]int

func readIndexesFromHeaderRow(sheet *xlsx.Sheet) columnIndexes {
	m := columnIndexes{}
	for i, cell := range sheet.Rows[0].Cells {
		m[cell.Value] = i
	}
	return m
}

func (cn columnIndexes) getString(key string, cells []*xlsx.Cell) string {
	if cellIndex, ok := cn[key]; ok && cellIndex < len(cells) {
		return cells[cellIndex].Value
	}
	return ""
}

func (cn columnIndexes) getListOfCategory(key string, cells []*xlsx.Cell) SetOfStringDef {
	realElems := SetOfStringDef{}
	s := cn.getString(key, cells)
	elems := strings.Split(s, "|")
	for _, elem := range elems {
		s1 := strings.TrimSpace(elem)
		if s1 != "" {
			realElems[s1] = true
		}
	}
	return realElems
}

func (cn columnIndexes) getFloat(key string, cells []*xlsx.Cell, field string, rowNum int) float64 {
	parsedValue := float64(0)
	s := cn.getString(key, cells)
	if s != "" && s != "-" {
		var err error
		parsedValue, err = strconv.ParseFloat(s, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to parse Float, row: %d, field: %s, err: %s\n", rowNum, field, err)
			parsedValue = float64(0)
		}
	}
	return float64(parsedValue)
}

func (cn columnIndexes) getInt(key string, cells []*xlsx.Cell, field string, rowNum int) int {
	parsedValue := int64(0)
	s := cn.getString(key, cells)
	if s != "" && s != "-" {
		var err error
		parsedValue, err = strconv.ParseInt(s, 10, 32)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to parse Int, row: %d, field: %s, err: %s\n", rowNum, field, err)
			parsedValue = 0
		}
	}
	return int(parsedValue)
}

func (cn columnIndexes) getDate(key string, cells []*xlsx.Cell, field string, rowNum int) DateDef {
	s := cn.getString(key, cells)
	if s != "" && s != "-" {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return DateDef{unixMs(xlsx.TimeFromExcelTime(f, date1904))}
		}
		const shortForm = "2006-01-02"
		var err error
		var t time.Time
		if t, err = time.Parse(shortForm, s); err == nil {
			return DateDef{unixMs(t)}
		}
		fmt.Fprintf(os.Stderr, "Unable to parse Date, row: %d, field: %s, value: %s, err: %s\n", rowNum, field, s, err)
	}
	return DateDef{0}
}

func unixMs(t time.Time) int64 {
	return t.Unix() * 1e3
}
