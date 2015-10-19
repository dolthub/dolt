package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/attic-labs/noms/chunks"
	geo "github.com/attic-labs/noms/clients/gen/sha1_3bfd4da1c27a6472279b96d731b47e58e8832dee"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
)

// data can be obtained using:
// wget --output-document=sfcrime.csv https://data.sfgov.org/api/views/tmnf-yvry/rows.csv?accessType=DOWNLOAD
var (
	limitFlag    = flag.Int("limit", math.MaxInt32, "limit number of rows that are imported")
	inputFlag    = flag.String("input-file", "", "path to .csv file containing sfcrime data")
	quietFlag    = flag.Bool("quiet", false, "suppress printing of messages")
	numIncidents = 0
	rowsRead     = 0
	start        = time.Now()
)

const maxListSize = 1e5

type incidentWithIndex struct {
	incident *IncidentDef
	index    int
}

type refIndex struct {
	ref   types.Ref
	index int
}

type refIndexList []refIndex

func (a refIndexList) Len() int           { return len(a) }
func (a refIndexList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a refIndexList) Less(i, j int) bool { return a[i].index < a[j].index }

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	dsFlags := dataset.NewFlags()
	flag.Parse()
	ds := dsFlags.CreateDataset()
	if ds == nil || *inputFlag == "" {
		flag.Usage()
		return
	}
	defer ds.Close()

	csvfile, err := os.Open(*inputFlag)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer csvfile.Close()

	if util.MaybeStartCPUProfile() {
		defer util.StopCPUProfile()
	}

	reader := csv.NewReader(csvfile)

	minLon := float32(180)
	minLat := float32(90)
	maxLat := float32(-90)
	maxLon := float32(-180)

	// read the header row and discard it
	_, err = reader.Read()
	outLiers := 0
	limitExceeded := false
	iChan, rChan := getNomsWriter(ds.Store())
	refList := refIndexList{}
	refs := []types.Value{}

	// Start a go routine to add incident refs to the list as they are ready
	var refWg sync.WaitGroup
	refWg.Add(1)
	go func() {
		for ref := range rChan {
			refList = append(refList, ref)
		}
		sort.Sort(refList)
		for _, r := range refList {
			refs = append(refs, r.ref)
		}
		refWg.Done()
	}()

	index := 0

	for r, err := reader.Read(); !limitExceeded && err == nil; r, err = reader.Read() {
		rowsRead++
		id, _ := strconv.ParseInt(r[0], 10, 64)
		lon64, _ := strconv.ParseFloat(r[9], 32)
		lat64, _ := strconv.ParseFloat(r[10], 32)
		geopos := geo.GeopositionDef{Latitude: float32(lat64), Longitude: float32(lon64)}
		incident := IncidentDef{
			ID:          id,
			Category:    r[1],
			Description: r[2],
			DayOfWeek:   r[3],
			Date:        r[4],
			Time:        r[5],
			PdDistrict:  r[6],
			Resolution:  r[7],
			Address:     r[8],
			Geoposition: geopos,
			PdID:        r[12],
		}

		if geopos.Latitude > 35 && geopos.Latitude < 40 && geopos.Longitude > -125 && geopos.Longitude < 120 {
			minLat = min(minLat, geopos.Latitude)
			maxLat = max(maxLat, geopos.Latitude)
			minLon = min(minLon, geopos.Longitude)
			maxLon = max(maxLon, geopos.Longitude)
			iChan <- incidentWithIndex{&incident, index}
			index++
		} else {
			outLiers++
		}
		if !*quietFlag && rowsRead%maxListSize == 0 {
			fmt.Printf("Processed %d rows, %d incidents, elapsed time: %.2f secs\n", rowsRead, numIncidents, time.Now().Sub(start).Seconds())
		}
		if rowsRead >= *limitFlag {
			limitExceeded = true
		}
	}

	close(iChan)
	refWg.Wait()

	incidentRefs := types.NewList(refs...)
	if !*quietFlag {
		fmt.Printf("Converting refs list to noms list: %.2f secs\n", time.Now().Sub(start).Seconds())
	}
	_, ok := ds.Commit(incidentRefs)
	d.Exp.True(ok, "Could not commit due to conflicting edit")

	if !*quietFlag {
		fmt.Printf("Commit completed, elaspsed time: %.2f secs\n", time.Now().Sub(start).Seconds())
		printDataStats(rowsRead, numIncidents, outLiers, minLat, minLon, maxLat, maxLon)
	}

	fmt.Printf("Ref of list containing Incidents: %s, , elaspsed time: %.2f secs\n", incidentRefs.Ref(), time.Now().Sub(start).Seconds())
}

func getNomsWriter(cs chunks.ChunkSink) (iChan chan incidentWithIndex, rChan chan refIndex) {
	iChan = make(chan incidentWithIndex, 3000)
	rChan = make(chan refIndex, 3000)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			for incidentRecord := range iChan {
				v := incidentRecord.incident.New()
				r := types.WriteValue(v.NomsValue(), cs)
				rChan <- refIndex{types.Ref{R: r}, incidentRecord.index}
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(rChan)
	}()

	return
}

func printDataStats(rows, rowsInserted, badRows int, minLat, minLon, maxLat, maxLon float32) {
	template := `Dataset Info:
    Number of rows: %d
    Incidents inserted: %d
    Incidents not inserted due to outlying coordinates: %d
    Minimum latitude: %f,
    Minimum longitude: %f
    Maximum latitude: %f
    Maximum longitude: %f
    `
	fmt.Printf(template, rows, rowsInserted, badRows, minLat, minLon, maxLat, maxLon)
	fmt.Printf("Minimum Georectangle(%.2f, %.2f, %.2f, %.2f)\n",
		toFixedCeil(float32(maxLat), 2), toFixedFloor(float32(minLon), 2), toFixedFloor(float32(minLat), 2), toFixedCeil(float32(maxLon), 2))
}

// Utility functions
func min(x, y float32) float32 {
	return float32(math.Min(float64(x), float64(y)))
}

func max(x, y float32) float32 {
	return float32(math.Max(float64(x), float64(y)))
}

func toFixedFloor(num float32, precision int) float32 {
	output := math.Pow(10, float64(precision))
	return float32(math.Floor(float64(num)*output) / output)
}

func toFixedCeil(num float32, precision int) float32 {
	output := math.Pow(10, float64(precision))
	return float32(math.Ceil(float64(num)*output) / output)
}
