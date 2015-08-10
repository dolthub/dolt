package main

import (
	"flag"
	"fmt"
	"log"
	"reflect"
	"strconv"

	"github.com/attic-labs/noms/chunks"
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
	// Walk through the list in inputDataset and basically switch
	// on the top-level key to know if it's an inning or a pitcher.
	pitchCounts := NewMapOfStringToListOfPitch()
	pitchers := NewMapOfStringToString()
	for i := uint64(0); i < input.Len(); i++ {
		m := input.Get(i).(types.Map)

		// TODO: This really sucks
		input.Release()

		if key := types.NewString("inning"); m.Has(key) {
			for idStr, p := range processInning(m.Get(key).(types.Map)) {
				id := types.NewString(idStr)
				pitches := NewListOfPitch()
				if pitchCounts.Has(id) {
					pitches = pitchCounts.Get(id)
				}
				pitchCounts = pitchCounts.Set(id, pitches.Append(p...))
			}
		} else if key := types.NewString("Player"); m.Has(key) {
			id, name := processPitcher(m.Get(key).(types.Map))
			if id.String() != "" && name.String() != "" {
				pitchers = pitchers.Set(id, name)
			}
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
	csFlags := chunks.NewFlags()
	flag.Parse()

	cs := csFlags.CreateStore()
	if cs == nil || *inputID == "" || *outputID == "" {
		flag.Usage()
		return
	}
	started := false
	if err := d.Try(func() { started = util.MaybeStartCPUProfile() }); started {
		defer util.StopCPUProfile()
	} else if err != nil {
		log.Fatalf("Can't create cpu profile file:\n%v\n", err)
	}

	dataStore := datas.NewDataStore(cs)
	inputDataset := dataset.NewDataset(dataStore, *inputID)
	outputDataset := dataset.NewDataset(dataStore, *outputID)

	input := types.ListFromVal(inputDataset.Heads().Any().Value())
	output := getIndex(input)

	outputDataset.Commit(datas.NewSetOfCommit().Insert(
		datas.NewCommit().SetParents(outputDataset.Heads().NomsValue()).SetValue(output.NomsValue())))

	if err := d.Try(util.MaybeWriteMemProfile); err != nil {
		log.Fatalf("Can't create memory profile file:\n%v\n", err)
	}
}
