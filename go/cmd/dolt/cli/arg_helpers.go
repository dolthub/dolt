package cli

import (
	"flag"
	"github.com/liquidata-inc/ld/dolt/go/libraries/set"
	"strings"
)

// BoolFlagMap holds a map of flag names to their value pointers which should be filled in by a call to
// flag.Flagset.Parse(args)
type BoolFlagMap struct {
	flags map[string]*bool
}

// NewBoolFlagMap iterates over all the argument name and argument description pairs provided in the nameToDesc map
// and creates a bool flag using the flagset.  The pointer to the value is stored in an internal map which lives
// within the instance that is returned. After Parse(args) is called on the flag.FlagSet the values of the flags within
// the map will be updated and can be retrieved using GetEqualTo
func NewBoolFlagMap(fs *flag.FlagSet, nameToDesc map[string]string) *BoolFlagMap {
	flags := make(map[string]*bool)
	for k, v := range nameToDesc {
		flags[k] = fs.Bool(k, false, v)
	}

	return &BoolFlagMap{flags}
}

// GetEqualTo returns a slice of all the names of the flags whose value is equal to the testVal provided.
func (bfm *BoolFlagMap) GetEqualTo(testVal bool) *set.StrSet {
	names := make([]string, 0, len(bfm.flags))
	for k, v := range bfm.flags {
		if *v == testVal {
			names = append(names, k)
		}
	}

	return set.NewStrSet(names)
}

func (bfm *BoolFlagMap) Get(flagName string) bool {
	return *bfm.flags[flagName]
}

type StrArgMap struct {
	args map[string]*string

	emptyArgs *set.StrSet
}

func NewStrArgMap(fs *flag.FlagSet, nameToDesc map[string]string) *StrArgMap {
	flags := make(map[string]*string)
	for k, v := range nameToDesc {
		flags[k] = fs.String(k, "", v)
	}

	return &StrArgMap{flags, nil}
}

func (sfm *StrArgMap) Update() {
	sfm.emptyArgs = set.NewStrSet([]string{})
	for k, v := range sfm.args {
		cleanVal := ""
		if v != nil {
			cleanVal = strings.TrimSpace(*v)
		}

		sfm.args[k] = &cleanVal

		if cleanVal == "" {
			sfm.emptyArgs.Add(k)
		}
	}
}

func (sfm *StrArgMap) GetEmpty() *set.StrSet {
	return sfm.emptyArgs
}

func (sfm *StrArgMap) Get(param string) string {
	return *sfm.args[param]
}
