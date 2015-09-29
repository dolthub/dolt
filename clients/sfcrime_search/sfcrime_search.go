package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"net"
	"net/http"
	"sort"
	"strings"
)

var (
	datasFlags      = datas.NewFlags()
	quadTreeRefFlag = flag.String("quadtree-ref", "", "ref containing root of quadtree")
	latFlag         = flag.Float64("lat", 0.0, "latitude of point to search for crime instances")
	lonFlag         = flag.Float64("lon", 0.0, "longitude of point to search for crime instances")
	distanceFlag    = flag.Float64("distance", 0.5, "distince in kilometers from point to search for crime instances")
	sqtRoot         SQuadTree
)

const (
	maxRequests = 8
	searchPath  = "/s/"
)

type httpServer struct {
	port       int
	l          *net.Listener
	conns      map[net.Conn]http.ConnState
	writeLimit chan struct{}
}

func main() {
	flag.Parse()
	start := time.Now()

	datastore, ok := datasFlags.CreateDataStore()
	if !ok || *quadTreeRefFlag == "" {
		flag.Usage()
		return
	}
	defer datastore.Close()

	var qtRef ref.Ref
	err := d.Try(func() {
		qtRef = ref.Parse(*quadTreeRefFlag)
	})
	if err != nil {
		log.Fatalf("Invalid quadtree-ref: %v", *quadTreeRefFlag)
	}
	qtVal := types.ReadValue(qtRef, datastore)
	sqtRoot = SQuadTreeFromVal(qtVal)

	if *latFlag == 0.0 || *lonFlag == 0.0 {
		flag.Usage()
		return
	}

	gp := GeopositionDef{float32(*latFlag), float32(*lonFlag)}
	if !sqtRoot.Georectangle().Def().ContainsPoint(gp) {
		fmt.Printf("lat/lon: %+v is not within sf area: %+v\n", gp, sqtRoot.Georectangle().Def())
		return
	}

	gr, incidents := sqtRoot.Query(gp, *distanceFlag)
	fmt.Printf("bounding Rectangle: %+v, numIncidents: %d\n", gr, len(incidents))
	var resDefs []IncidentDef
	for _, incident := range incidents {
		resDefs = append(resDefs, incident.Def())
	}
	sort.Sort(sort.Reverse(ByDate(resDefs)))
	for _, n := range resDefs {
		fmt.Printf("Incident, date: %s, category: %s, desc: %s, address: %s\n", n.Date, n.Category, n.Description, n.Address)
	}
	fmt.Printf("Done, elapsed time: %.2f secs\n", time.Now().Sub(start).Seconds())
}

type ByDate []IncidentDef

func (s ByDate) Len() int {
	return len(s)
}
func (s ByDate) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s ByDate) Less(i, j int) bool {
	i1 := strings.Split(s[i].Date, "/")
	idate := fmt.Sprintf("%s/%s/%s", i1[2], i1[0], i1[1])
	j1 := strings.Split(s[j].Date, "/")
	jdate := fmt.Sprintf("%s/%s/%s", j1[2], j1[0], j1[1])
	return idate < jdate
}
