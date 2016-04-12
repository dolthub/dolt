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

func processPitcher(m MapOfStringToValue) (id, name string) {
	id = m.getAsString("-id")
	name = fmt.Sprintf("%s, %s", m.getAsString("-last_name"), m.getAsString("-first_name"))
	return
}

func checkPitch(v MapOfStringToValue) bool {
	return v.Has("-px") && v.Has("-pz")
}

func getPitch(v MapOfStringToValue) PitchDef {
	x, _ := strconv.ParseFloat(v.getAsString("-px"), 64)
	z, _ := strconv.ParseFloat(v.getAsString("-pz"), 64)
	return PitchDef{X: x, Z: z}
}

func processPitches(v types.Value) (pitches []PitchDef) {
	switch v := v.(type) {
	case types.List:
		for i := uint64(0); i < v.Len(); i++ {
			pitches = append(pitches, processPitches(v.Get(i))...)
		}
	case MapOfStringToValue:
		if checkPitch(v) {
			pitches = append(pitches, getPitch(v))
		}
	case nil:
		return // Yes, an at-bat can end with no pitches thrown.
	default:
		d.Chk.Fail("Impossible pitch", "No pitch should be %+v, which is of type %s!\n", v, reflect.TypeOf(v).String())
	}
	return
}

func (m MapOfStringToValue) getAsString(k string) string {
	return m.Get(k).(types.String).String()
}

func processInning(m MapOfStringToValue) map[string][]PitchDef {
	pitchCounts := map[string][]PitchDef{}

	// This is brittle, figure out how to do it without being super verbose.
	top, ok := m.Get("top").(MapOfStringToValue)
	if !ok {
		// If "top" is anything other than a map, give up
		return pitchCounts
	}

	halves := []MapOfStringToValue{top}

	if bot := m.Get("bottom"); bot != nil {
		halves = append(halves, bot.(MapOfStringToValue))
	}

	addPitch := func(ab MapOfStringToValue) {
		pitchData := ab.Get("pitch")
		pitcher := ab.Get("-pitcher").(types.String).String()
		pitchCounts[pitcher] = append(pitchCounts[pitcher], processPitches(pitchData)...)
	}

	for _, half := range halves {
		atbat := half.Get("atbat")
		switch atbat := atbat.(type) {
		case types.List:
			for i := uint64(0); i < atbat.Len(); i++ {
				addPitch(atbat.Get(i).(MapOfStringToValue))
			}
		case MapOfStringToValue:
			// Apparently, if there's only one, it's encoded directly as a singleton. Yay, data!
			addPitch(atbat)
		default:
			d.Chk.Fail("Impossible half", "No half should be %+v, which is of type %s!\n", atbat, reflect.TypeOf(atbat).String())
		}
	}
	return pitchCounts
}

func getIndex(input ListOfRefOfMapOfStringToValue, vrw types.ValueReadWriter) MapOfStringToRefOfListOfPitch {
	pitcherMu := sync.Mutex{}
	inningMu := sync.Mutex{}
	pitchers := map[string]string{}
	innings := []map[string][]PitchDef{}

	// Walk through the list in inputDataset and basically switch
	// on the top-level key to know if it's an inning or a pitcher.
	input.IterAllP(512, func(item RefOfMapOfStringToValue, i uint64) {
		m := item.TargetValue(vrw)

		if key := "inning"; m.Has(key) {
			inning := processInning(m.Get(key).(MapOfStringToValue))
			inningMu.Lock()
			innings = append(innings, inning)
			inningMu.Unlock()
		}

		if key := "Player"; m.Has(key) {
			id, name := processPitcher(m.Get(key).(MapOfStringToValue))

			if id != "" && name != "" {
				pitcherMu.Lock()
				pitchers[id] = name
				pitcherMu.Unlock()
			}
		}
	})

	pitchCounts := map[string]ListOfPitchDef{}
	for _, inning := range innings {
		for id, p := range inning {
			pitchCounts[id] = append(pitchCounts[id], p...)
		}
	}

	namedPitchCounts := MapOfStringToRefOfListOfPitchDef{}
	for id, p := range pitchCounts {
		if name, ok := pitchers[id]; d.Chk.True(ok, "Unknown pitcher: %s", id) {
			namedPitchCounts[name] = vrw.WriteValue(p.New()).TargetRef()
		}
	}
	return namedPitchCounts.New()
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
		inputDataset := dataset.NewDataset(ds, *inputID)
		outputDataset := dataset.NewDataset(ds, *outputID)

		input := inputDataset.Head().Value().(ListOfRefOfMapOfStringToValue)
		output := getIndex(input, ds)

		_, err := outputDataset.Commit(output)
		d.Exp.NoError(err)

		util.MaybeWriteMemProfile()
	})
	if err != nil {
		log.Fatal(err)
	}
}
