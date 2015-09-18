package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	"io"
	"math"
	"os"
	"strconv"
	"time"
)

var (
	limitFlag = flag.Int("limit", math.MaxInt32, "limit number of rows that are imported")
    inputFlag = flag.String("input-file", "", "path to .csv file containing sfcrime data")
)

func main() {
	dsFlags := dataset.NewFlags()
	flag.Parse()
	ds := dsFlags.CreateDataset()
	if ds == nil || *inputFlag == "" {
		flag.Usage()
		return
	}
	defer ds.Close()

	start := time.Now()
	csvfile, err := os.Open(*inputFlag)

	if err != nil {
		fmt.Println(err)
		return
	}

	defer csvfile.Close()

	reader := csv.NewReader(csvfile)

	minLon := types.Float32(180)
	minLat := types.Float32(90)
	maxLat := types.Float32(-90)
	maxLon := types.Float32(-180)
	_, err = reader.Read()
	incidents := []types.Value{}
	numBadRows := 0
	numDataRows := 0
	cont := true
	for r, err := reader.Read(); cont && err == nil; r, err = reader.Read() {
		numDataRows++
		id, _ := strconv.ParseInt(r[0], 10, 64)
		lon64, _ := strconv.ParseFloat(r[9], 32)
		lat64, _ := strconv.ParseFloat(r[10], 32)
		lon := types.Float32(float32(lon64))
		lat := types.Float32(float32(lat64))
		geoposition := NewGeoposition().
			SetLatitude(lat).
			SetLongitude(lon)
		incident := NewIncident().
			SetID(types.Int64(id)).
			SetCategory(types.NewString(r[1])).
			SetDescription(types.NewString(r[2])).
			SetDayOfWeek(types.NewString(r[3])).
			SetDate(types.NewString(r[4])).
			SetTime(types.NewString(r[5])).
			SetPdDistrict(types.NewString(r[6])).
			SetResolution(types.NewString(r[7])).
			SetAddress(types.NewString(r[8])).
			SetGeoposition(geoposition).
			SetPdID(types.NewString(r[12]))

		if lat > 35 && lat < 40 && lon > -125 && lon < 120 {
			minLat = min(minLat, lat)
			maxLat = max(maxLat, lat)
			minLon = min(minLon, lon)
			maxLon = max(maxLon, lon)

			incidents = append(incidents, incident.NomsValue())
		} else {
			numBadRows++
			//			fmt.Println("Bad lat/lon value in row:", i, "lat:", y, "lon:", x)
		}
		if len(incidents)%1e5 == 0 {
			fmt.Printf("Added %d incidents, elapsed time: %.2f secs\n", len(incidents), time.Now().Sub(start).Seconds())
		}
		if len(incidents) >= *limitFlag {
			cont = false
		}
	}

    printDataStats(numDataRows, len(incidents), numBadRows, minLat, minLon, maxLat, maxLon)
    if err != nil && err != io.EOF {
        fmt.Println(err)
        os.Exit(1)
    }

	nomsIncidents := types.NewList(incidents...)
	fmt.Printf("Incident slice converted to types.list, elapsedTime: %.2f secs\n", time.Now().Sub(start).Seconds())
	_, ok := ds.Commit(nomsIncidents)
    d.Exp.True(ok, "Could not commit due to conflicting edit")
	
    fmt.Printf("Commit completed, elaspsed time: %.2f secs\n", time.Now().Sub(start).Seconds())
    fmt.Println("Ref of list containing Incidents:", nomsIncidents.Ref())
}

func printDataStats(rows, rowsInserted, badRows int, minLat, minLon, maxLat, maxLon types.Float32) {
	template := `Dataset Info:
    Number of rows: %d
    Incidents inserted: %d
    Rows skipped because of bad data: %d
    Minimum latitude: %f,
    Minimum longitude: %f
    Maximum latitude: %f
    Maximum longitude: %f
    `
	fmt.Printf(template, rows, rowsInserted, badRows, minLat, minLon, maxLat, maxLon)

	fmt.Printf("Minimum Georectangle(%.2f, %.2f, %.2f,%.2f)\n",
		toFixedCeil(float32(maxLat), 2), toFixedFloor(float32(minLon), 2), toFixedFloor(float32(minLat), 2), toFixedCeil(float32(maxLon), 2))
}

// Utility functions
func min(x, y types.Float32) types.Float32 {
	return types.Float32(math.Min(float64(x), float64(y)))
}

func max(x, y types.Float32) types.Float32 {
	return types.Float32(math.Max(float64(x), float64(y)))
}

func (p Geoposition) String() string {
	return fmt.Sprintf("Geoposition(lat: %f, lon: %f", p.Latitude(), p.Longitude())
}

func toFixedFloor(num float32, precision int) float32 {
	output := math.Pow(10, float64(precision))
	return float32(math.Floor(float64(num)*output) / output)
}

func toFixedCeil(num float32, precision int) float32 {
	output := math.Pow(10, float64(precision))
	return float32(math.Ceil(float64(num)*output) / output)
}
