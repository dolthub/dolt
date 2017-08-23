package query

import (
	"strings"
	"testing"
)

type filterTestCase struct {
	filter Filter
	keys   []string
	expect []string
}

func testKeyFilter(t *testing.T, f Filter, keys []string, expect []string) {
	e := make([]Entry, len(keys))
	for i, k := range keys {
		e[i] = Entry{Key: k}
	}

	res := ResultsWithEntries(Query{}, e)
	res = NaiveFilter(res, f)
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

func TestFilterKeyCompare(t *testing.T) {

	testKeyFilter(t, FilterKeyCompare{Equal, "/ab"}, sampleKeys, []string{"/ab"})
	testKeyFilter(t, FilterKeyCompare{GreaterThan, "/ab"}, sampleKeys, []string{
		"/ab/c",
		"/ab/cd",
		"/abce",
		"/abcf",
	})
	testKeyFilter(t, FilterKeyCompare{LessThanOrEqual, "/ab"}, sampleKeys, []string{
		"/a",
		"/ab",
	})
}

func TestFilterKeyPrefix(t *testing.T) {

	testKeyFilter(t, FilterKeyPrefix{"/a"}, sampleKeys, []string{
		"/ab/c",
		"/ab/cd",
		"/a",
		"/abce",
		"/abcf",
		"/ab",
	})
	testKeyFilter(t, FilterKeyPrefix{"/ab/"}, sampleKeys, []string{
		"/ab/c",
		"/ab/cd",
	})
}
