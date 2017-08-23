package record

import (
	"errors"
	"strings"
)

// A SelectorFunc selects the best value for the given key from
// a slice of possible values and returns the index of the chosen one
type SelectorFunc func(string, [][]byte) (int, error)

type Selector map[string]SelectorFunc

func (s Selector) BestRecord(k string, recs [][]byte) (int, error) {
	if len(recs) == 0 {
		return 0, errors.New("no records given!")
	}

	parts := strings.Split((string(k)), "/")
	if len(parts) < 3 {
		log.Infof("Record key does not have selectorfunc: %s", k)
		return 0, errors.New("record key does not have selectorfunc")
	}

	sel, ok := s[parts[1]]
	if !ok {
		log.Infof("Unrecognized key prefix: %s", parts[1])
		return 0, ErrInvalidRecordType
	}

	return sel(k, recs)
}

// PublicKeySelector just selects the first entry.
// All valid public key records will be equivalent.
func PublicKeySelector(k string, vals [][]byte) (int, error) {
	return 0, nil
}
