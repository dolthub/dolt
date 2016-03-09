package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/attic-labs/noms/clients/common"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

var (
	datasFlags          = datas.NewFlags()
	quadTreeRefFlag     = flag.String("quadtree-ref", "", "ref to root of quadtree")
	incidentListRefFlag = flag.String("incident-list-ref", "", "ref to list of incidents")
	latFlag             = flag.Float64("lat", 0.0, "latitude of point to search for crime instances")
	lonFlag             = flag.Float64("lon", 0.0, "longitude of point to search for crime instances")
	distanceFlag        = flag.Float64("distance", 0.5, "distince in kilometers from point to search for crime instances")
	sqtRoot             common.SQuadTree
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
	if !ok {
		flag.Usage()
		return
	}
	defer datastore.Close()

	if *latFlag == 0.0 || *lonFlag == 0.0 {
		flag.Usage()
		return
	}

	gp := common.GeopositionDef{Latitude: float32(*latFlag), Longitude: float32(*lonFlag)}
	var incidents []common.Incident
	if *quadTreeRefFlag != "" {
		incidents = searchWithQuadTree(gp, datastore)
	} else if *incidentListRefFlag != "" {
		incidents = searchWithList(gp, datastore)
	} else {
		fmt.Println("You must supply either the 'quadtree-ref' or the 'incident-list-ref' argumements")
		flag.Usage()
		return
	}

	var resDefs []common.IncidentDef
	for _, incident := range incidents {
		resDefs = append(resDefs, incident.Def())
	}
	sort.Sort(sort.Reverse(ByDate(resDefs)))

	for _, n := range resDefs {
		fmt.Printf("Incident, date: %s, category: %s, desc: %s, address: %s\n", n.Date, n.Category, n.Description, n.Address)
	}
	fmt.Printf("Done, elapsed time: %.2f secs\n", time.Now().Sub(start).Seconds())
}

func searchWithQuadTree(gp common.GeopositionDef, vr types.ValueReader) []common.Incident {
	argName := "quadtree-ref"
	r := readRef(*quadTreeRefFlag, argName)
	sqtRoot := vr.ReadValue(r).(common.SQuadTree)
	if !common.ContainsPoint(sqtRoot.Georectangle().Def(), gp) {
		log.Fatalf("lat/lon: %+v is not within sf area: %+v\n", gp, sqtRoot.Georectangle().Def())
	}
	gr, results := sqtRoot.Query(gp, *distanceFlag, vr)
	fmt.Printf("bounding Rectangle: %+v, numIncidents: %d\n", gr, len(results))
	return results
}

func searchWithList(gp common.GeopositionDef, vr types.ValueReader) []common.Incident {
	argName := "incident-list-ref"
	r := readRef(*incidentListRefFlag, argName)
	val := vr.ReadValue(r)
	l, ok := val.(types.List)
	if !ok {
		log.Fatalf("Value for %s argument is not a list object\n", argName)
	}
	if l.Len() == 0 {
		log.Fatalf("Value for %s argument is an empty list\n", argName)
	}
	results := []common.Incident{}
	incidentList := val.(common.ListOfRefOfValue)
	t0 := time.Now()
	for i := uint64(0); i < incidentList.Len(); i++ {
		if i%uint64(10000) == 0 {
			fmt.Printf("%.2f%%: %v\n", float64(i)/float64(incidentList.Len())*float64(100), time.Now().Sub(t0))
		}
		incident := incidentList.Get(i).TargetValue(vr).(common.Incident)
		if common.DistanceTo(incident.Geoposition().Def(), gp) <= float32(*distanceFlag) {
			results = append(results, incident)
		}
	}
	return results
}

func readRef(rs string, argName string) ref.Ref {
	var r ref.Ref
	err := d.Try(func() {
		r = ref.Parse(rs)
	})
	if err != nil {
		log.Fatalf("Invalid ref for %s arg: %v", argName, *quadTreeRefFlag)
	}
	return r
}

type ByDate []common.IncidentDef

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
