package main

import (
	"flag"
	"fmt"
	"reflect"
	"strconv"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/types"
)

var (
	inputID  = flag.String("input-dataset-id", "", "dataset to pull data from.")
	outputID = flag.String("output-dataset-id", "", "dataset to store data in.")
)

func getAsString(fm types.Map, key string) string {
	v := fm.Get(types.NewString(key))
	if v == nil {
		return ""
	}
	return v.(types.String).String()
}

func processPitcher(m types.Map) (id, name types.String) {
	id = types.NewString(getAsString(m, "-id"))
	name = types.NewString(fmt.Sprintf("%s, %s", getAsString(m, "-last_name"), getAsString(m, "-first_name")))
	return
}

func toPitch(v types.Map) Pitch {
	x, _ := strconv.ParseFloat(v.Get(types.NewString("-px")).(types.String).String(), 64)
	z, _ := strconv.ParseFloat(v.Get(types.NewString("-pz")).(types.String).String(), 64)
	return NewPitch().SetX(types.Float64(x)).SetZ(types.Float64(z))
}

func processPitches(v types.Value) (pitches []Pitch) {
	switch v := v.(type) {
	case types.List:
		for i := uint64(0); i < v.Len(); i++ {
			pitches = append(pitches, processPitches(v.Get(i))...)
		}
	case types.Map:
		pitches = append(pitches, toPitch(v))
	default:
		dbg.Chk.Fail("pitches shouldn't be %+v, which is of type %s!\n", v, reflect.TypeOf(v).String())
	}
	return
}

func getPitchesByPitcherForInning(m types.Map) map[string][]Pitch {
	// This is brittle, figure out how to do it without being super verbose.
	halves := []types.Map{
		m.Get(types.NewString("top")).(types.Map),
		m.Get(types.NewString("bottom")).(types.Map),
	}
	pitchCounts := map[string][]Pitch{}
	for _, half := range halves {
		abs := half.Get(types.NewString("atbat")).(types.List)
		for i := uint64(0); i < abs.Len(); i++ {
			ab := abs.Get(i).(types.Map)
			pitchData := ab.Get(types.NewString("pitch"))
			pitcher := ab.Get(types.NewString("-pitcher")).(types.String).String()
			pitchCounts[pitcher] = append(pitchCounts[pitcher], processPitches(pitchData)...)
		}
	}
	return pitchCounts
}

func main() {
	csFlags := chunks.NewFlags()
	flag.Parse()

	cs := csFlags.CreateStore()
	if cs == nil || *inputID == "" || *outputID == "" {
		flag.Usage()
		return
	}

	rootDataStore := datas.NewDataStore(cs, cs.(chunks.RootTracker))
	inputDataset := dataset.NewDataset(rootDataStore, *inputID)
	outputDataset := dataset.NewDataset(rootDataStore, *outputID)

	// Walk through the list in inputDataset and basically switch
	// on the top-level key to know if it's an inning or a pitcher.
	l := inputDataset.Roots().Any().Value().(types.List)
	pitchCounts := types.NewMap()
	pitchers := types.NewMap()
	for i := uint64(0); i < l.Len(); i++ {
		m := l.Get(i).(types.Map)
		if key := types.NewString("inning"); m.Has(key) {
			for idStr, p := range getPitchesByPitcherForInning(m.Get(key).(types.Map)) {
				id := types.NewString(idStr)
				pitches := NewPitchList()
				if pitchCounts.Has(id) {
					pitches = PitchListFromVal(pitchCounts.Get(id))
				}
				pitchCounts = pitchCounts.Set(id, pitches.Append(p...).NomsValue())
			}
		} else if key := types.NewString("Player"); m.Has(key) {
			id, name := processPitcher(m.Get(key).(types.Map))
			if id.String() != "" && name.String() != "" {
				pitchers = pitchers.Set(id, name)
			}
		}
	}

	namedPitchCounts := types.NewMap()
	pitchCounts.Iter(func(id, p types.Value) (stop bool) {
		if pitchers.Has(id) {
			namedPitchCounts = namedPitchCounts.Set(pitchers.Get(id), p)
		} else {
			dbg.Chk.Fail("Unknown pitcher!", id)
		}
		return
	})
	outputDataset.Commit(datas.NewRootSet().Insert(
		datas.NewRoot().SetParents(outputDataset.Roots().NomsValue()).SetValue(namedPitchCounts)))
}
