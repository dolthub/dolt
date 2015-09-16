package main

import (
	"flag"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"strconv"
	"sync"

	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
)

var (
	inputID  = flag.String("input-ds", "", "dataset to pull data from.")
	outputID = flag.String("output-ds", "", "dataset to store data in.")
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

func checkPitch(v types.Map) bool {
	return v.Get(types.NewString("-px")) != nil && v.Get(types.NewString("-pz")) != nil
}

func getPitch(v types.Map) Pitch {
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
		if checkPitch(v) {
			pitches = append(pitches, getPitch(v))
		}
	case nil:
		return // Yes, an at-bat can end with no pitches thrown.
	default:
		d.Chk.Fail("No pitch should be %+v, which is of type %s!\n", v, reflect.TypeOf(v).String())
	}
	return
}

func processInning(m types.Map) map[string][]Pitch {
	pitchCounts := map[string][]Pitch{}

	// This is brittle, figure out how to do it without being super verbose.
	top := m.Get(types.NewString("top"))
	switch top.(type) {
	case types.Map:
	default:
		// If "top" is anything other than a map, give up
		return pitchCounts
	}

	halves := []types.Map{
		top.(types.Map),
	}

	if bot := m.Get(types.NewString("bottom")); bot != nil {
		halves = append(halves, bot.(types.Map))
	}

	addPitch := func(ab types.Map) {
		pitchData := ab.Get(types.NewString("pitch"))
		pitcher := ab.Get(types.NewString("-pitcher")).(types.String).String()
		pitchCounts[pitcher] = append(pitchCounts[pitcher], processPitches(pitchData)...)
	}

	for _, half := range halves {
		atbat := half.Get(types.NewString("atbat"))
		switch atbat.(type) {
		case types.List:
			abs := atbat.(types.List)
			for i := uint64(0); i < abs.Len(); i++ {
				ab := abs.Get(i).(types.Map)
				addPitch(ab)
			}
		case types.Map:
			// Apparently, if there's only one, it's encoded directly as a singleton. Yay, data!
			addPitch(atbat.(types.Map))
		default:
		}
	}
	return pitchCounts
}

func getIndex(input types.List) MapOfStringToListOfPitch {
	mu := sync.Mutex{}
	pitchers := NewMapOfStringToString()

	// Walk through the list in inputDataset and basically switch
	// on the top-level key to know if it's an inning or a pitcher.
	innings := input.MapP(512, func(item types.Value) interface{} {
		m := item.(types.Map)

		if key := types.NewString("inning"); m.Has(key) {
			return processInning(m.Get(key).(types.Map))
		}

		if key := types.NewString("Player"); m.Has(key) {
			id, name := processPitcher(m.Get(key).(types.Map))
			if id.String() != "" && name.String() != "" {
				mu.Lock()
				pitchers = pitchers.Set(id, name)
				mu.Unlock()
			}
		}

		return nil
	})

	pitchCounts := NewMapOfStringToListOfPitch()
	for _, inning := range innings {
		if inning == nil {
			continue
		}

		for idStr, p := range inning.(map[string][]Pitch) {
			id := types.NewString(idStr)
			pitches := NewListOfPitch()
			if pitchCounts.Has(id) {
				pitches = pitchCounts.Get(id)
			}
			pitchCounts = pitchCounts.Set(id, pitches.Append(p...))
		}
	}

	namedPitchCounts := NewMapOfStringToListOfPitch()
	pitchCounts.Iter(func(id types.String, p ListOfPitch) (stop bool) {
		if pitchers.Has(id) {
			namedPitchCounts = namedPitchCounts.Set(pitchers.Get(id), p)
		} else {
			d.Chk.Fail("Unknown pitcher!", id)
		}
		return
	})

	return namedPitchCounts
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flags := datas.NewFlags()
	flag.Parse()

	ds, ok := flags.CreateDataStore()
	if !ok || *inputID == "" || *outputID == "" {
		flag.Usage()
		return
	}
	defer ds.Close()

	err := d.Try(func() {
		if util.MaybeStartCPUProfile() {
			defer util.StopCPUProfile()
		}
		dataStore := datas.NewDataStore(ds)
		inputDataset := dataset.NewDataset(dataStore, *inputID)
		outputDataset := dataset.NewDataset(dataStore, *outputID)

		input := types.ListFromVal(inputDataset.Head().Value())
		output := getIndex(input).NomsValue()

		_, ok := outputDataset.Commit(output)
		d.Exp.True(ok, "Could not commit due to conflicting edit")

		util.MaybeWriteMemProfile()
	})
	if err != nil {
		log.Fatal(err)
	}
}
