package query

import (
	"strings"
	"testing"
	"reflect"
)

var sampleKeys = []string{
	"/ab/c",
	"/ab/cd",
	"/a",
	"/abce",
	"/abcf",
	"/ab",
}

type testCase struct {
	keys   []string
	expect []string
}

func testResults(t *testing.T, res Results, expect []string) {
	actualE, err := res.Rest()
	if err != nil {
		t.Fatal(err)
	}

	actual := make([]string, len(actualE))
	for i, e := range actualE {
		actual[i] = e.Key
	}

	if len(actual) != len(expect) {
		t.Error("expect != actual.", expect, actual)
	}

	if strings.Join(actual, "") != strings.Join(expect, "") {
		t.Error("expect != actual.", expect, actual)
	}
}

func TestLimit(t *testing.T) {
	testKeyLimit := func(t *testing.T, limit int, keys []string, expect []string) {
		e := make([]Entry, len(keys))
		for i, k := range keys {
			e[i] = Entry{Key: k}
		}

		res := ResultsWithEntries(Query{}, e)
		res = NaiveLimit(res, limit)
		testResults(t, res, expect)
	}

	testKeyLimit(t, 0, sampleKeys, []string{ // none
		"/ab/c",
		"/ab/cd",
		"/a",
		"/abce",
		"/abcf",
		"/ab",
	})

	testKeyLimit(t, 10, sampleKeys, []string{ // large
		"/ab/c",
		"/ab/cd",
		"/a",
		"/abce",
		"/abcf",
		"/ab",
	})

	testKeyLimit(t, 2, sampleKeys, []string{
		"/ab/c",
		"/ab/cd",
	})
}

func TestOffset(t *testing.T) {

	testOffset := func(t *testing.T, offset int, keys []string, expect []string) {
		e := make([]Entry, len(keys))
		for i, k := range keys {
			e[i] = Entry{Key: k}
		}

		res := ResultsWithEntries(Query{}, e)
		res = NaiveOffset(res, offset)
		testResults(t, res, expect)
	}

	testOffset(t, 0, sampleKeys, []string{ // none
		"/ab/c",
		"/ab/cd",
		"/a",
		"/abce",
		"/abcf",
		"/ab",
	})

	testOffset(t, 10, sampleKeys, []string{ // large
	})

	testOffset(t, 2, sampleKeys, []string{
		"/a",
		"/abce",
		"/abcf",
		"/ab",
	})
}

func TestResultsFromIterator(t *testing.T) {
	testResultsFromIteratorWClose(t, getKeysViaNextSync)
}

func TestResultsFromIteratorUsingChan(t *testing.T) {
	testResultsFromIteratorWClose(t, getKeysViaChan)
}

func TestResultsFromIteratorUsingRest(t *testing.T) {
	testResultsFromIteratorWClose(t, getKeysViaRest)
}

func TestResultsFromIteratorNoClose(t *testing.T) {
	testResultsFromIterator(t, getKeysViaNextSync, nil)
	testResultsFromIterator(t, getKeysViaChan, nil)
}

func testResultsFromIterator(t *testing.T, getKeys func(rs Results) []string, close func() error) {
	i := 0
	results := ResultsFromIterator(Query{}, Iterator{
		Next: func() (Result, bool) {
			if i >= len(sampleKeys) {
				return Result{}, false
			}
			res := Result{Entry: Entry{Key: sampleKeys[i]}}
			i++
			return res, true
		},
		Close: close,
	})
	keys := getKeys(results)
	if !reflect.DeepEqual(sampleKeys, keys) {
		t.Errorf("did not get the same set of keys")
	}
}

func testResultsFromIteratorWClose(t *testing.T, getKeys func(rs Results) []string) {
	closeCalled := 0
	testResultsFromIterator(t, getKeys, func() error {
		closeCalled++
		return nil
	})
	if closeCalled != 1 {
		t.Errorf("close called %d times, expect it to be called just once", closeCalled)
	}
}

func getKeysViaNextSync(rs Results) []string {
	ret := make([]string, 0)
	for {
		r, ok := rs.NextSync()
		if !ok {
			break
		}
		ret = append(ret, r.Key)
	}
	return ret
}

func getKeysViaRest(rs Results) []string {
	rest, _ := rs.Rest()
	ret := make([]string, 0)
	for _, e := range rest {
		ret = append(ret, e.Key)
	}
	return ret
}

func getKeysViaChan(rs Results) []string {
	ret := make([]string, 0)
	for r := range rs.Next() {
		ret = append(ret, r.Key)
	}
	return ret
}

