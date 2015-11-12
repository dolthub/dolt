package main

import (
	"flag"
	"fmt"
	"os"

	"strings"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/tealeg/xlsx"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
)

func main() {
	var ds *dataset.Dataset

	flag.Usage = func() {
		fmt.Println("Usage: crunchbase [options] file\n")
		flag.PrintDefaults()
	}

	dsFlags := dataset.NewFlags()
	flag.Parse()

	ds = dsFlags.CreateDataset()
	path := flag.Arg(0)
	if ds == nil || path == "" {
		flag.Usage()
		return
	}
	defer ds.Close()

	xlFile, err := xlsx.OpenFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(-1)
	}

	// Read in Rounds and group  according to CompanyPermalink
	roundsByPermalink := map[string][]Round{}
	roundsSheet := xlFile.Sheet["Rounds"]
	numRounds := 0
	for i, row := range roundsSheet.Rows {
		if i != 0 {
			var rounds []Round
			var ok bool

			round := NewRoundFromRow(row)
			if rounds, ok = roundsByPermalink[round.CompanyPermalink()]; !ok {
				rounds = []Round{}
			}
			rounds = append(rounds, round)
			roundsByPermalink[round._CompanyPermalink] = rounds
			numRounds++
		}
	}

	// Read in Companies and map to permalink
	companyRefs := NewMapOfStringToRefOfCompany()
	companySheet := xlFile.Sheet["Companies"]
	for i, row := range companySheet.Rows {
		if i != 0 {
			company := NewCompanyFromRow(row)
			permalink := company.Permalink()

			var rounds []Round
			var ok bool
			if rounds, ok = roundsByPermalink[permalink]; !ok {
				rounds = []Round{}
			}
			roundRefs := NewSetOfRefOfRound()
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

	// Write the list of companyRefs
	companiesRef := types.WriteValue(companyRefs, ds.Store())

	// Commit ref of the companiesRef list
	_, ok := ds.Commit(types.NewRef(companiesRef))
	d.Exp.True(ok, "Could not commit due to conflicting edit")

	fmt.Printf("### imported %d companies with %d rounds\n", companyRefs.Len(), numRounds)
}

func NewCompanyFromRow(row *xlsx.Row) Company {
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
		FoundedAt:       cells[12].Value,
		FoundedMonth:    cells[13].Value,
		FoundedYear:     cells[14].Value,
		FirstFundingAt:  cells[15].Value,
		LastFundingAt:   cells[16].Value,
	}
	return company.New()
}

func NewRoundFromRow(row *xlsx.Row) Round {
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
		FundedAt:              cells[11].Value,
		FundedMonth:           cells[12].Value,
		FundedQuarter:         cells[13].Value,
		FundedYear:            uint16(parseIntValue(cells[14], "Round.fundedYear")),
		RaisedAmountUsd:       raisedAmountUsd,
	}
	return round.New()
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
