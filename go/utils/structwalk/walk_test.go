package structwalk

import (
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestWalk(t *testing.T) {
	type innerStruct struct {
		Five *string `json:"five"`
	}

	type testStruct struct {
		One   string       `json:"one"`
		Two   int          `json:"two"`
		Three bool         `json:"three"`
		Four  *innerStruct `json:"four"`
		Six   []string     `json:"six"`
	}

	expected := []struct {
		name    string
		typeStr string
		depth   int
		json    string
	}{
		{
			name:    "One",
			typeStr: "string",
			depth:   0,
			json:    "one",
		},
		{
			name:    "Two",
			typeStr: "int",
			depth:   0,
			json:    "two",
		},
		{
			name:    "Three",
			typeStr: "bool",
			depth:   0,
			json:    "three",
		},
		{
			name:    "Four",
			typeStr: "*structwalk.innerStruct",
			depth:   0,
			json:    "four",
		},
		{
			name:    "Five",
			typeStr: "*string",
			depth:   1,
			json:    "five",
		},
		{
			name:    "Six",
			typeStr: "[]string",
			depth:   0,
			json:    "six",
		},
	}

	var n int
	err := Walk(&testStruct{}, func(sf reflect.StructField, depth int) error {
		require.Equal(t, expected[n].name, sf.Name)
		require.Equal(t, expected[n].typeStr, sf.Type.String())
		require.Equal(t, expected[n].depth, depth)
		require.Equal(t, expected[n].json, sf.Tag.Get("json"))
		n++
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, len(expected), n)
}
