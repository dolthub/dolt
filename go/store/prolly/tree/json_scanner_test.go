package tree

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type indexedSegment struct {
	path    [][]byte
	segment string
}
type jsonScannerTest struct {
	name     string
	json     string
	segments []indexedSegment
}

func TestJsonScanner(t *testing.T) {
	tests := []jsonScannerTest{
		{
			name: "simple string test",
			json: `"Hello, world"`,
			segments: []indexedSegment{
				{
					path:    [][]byte{[]byte("$")},
					segment: `"Hello, world"`,
				},
			},
		},
		{
			name: "simple number test",
			json: `1.1`,
			segments: []indexedSegment{
				{
					path:    [][]byte{[]byte("$")},
					segment: `1.1`,
				},
			},
		},
		{
			name: "empty array",
			json: "[]",
			segments: []indexedSegment{
				{
					path:    [][]byte{[]byte("$")},
					segment: "[]",
				},
			},
		},
		{
			name: "array with one child",
			json: `[1]`,
			segments: []indexedSegment{
				{
					path:    [][]byte{[]byte("$"), getVarInt(0)},
					segment: `[1`,
				},
				{
					path:    [][]byte{[]byte("$")},
					segment: `]`,
				},
			},
		},
		{
			name: "array with many children",
			json: `[1,2,3]`,
			segments: []indexedSegment{
				{
					path:    [][]byte{[]byte("$"), getVarInt(0)},
					segment: `[1`,
				},
				{
					path:    [][]byte{[]byte("$"), getVarInt(1)},
					segment: `,2`,
				},
				{
					path:    [][]byte{[]byte("$"), getVarInt(2)},
					segment: `,3`,
				},
				{
					path:    [][]byte{[]byte("$")},
					segment: `]`,
				},
			},
		},
		{
			name: "nested empty array",
			json: `[[]]`,
			segments: []indexedSegment{
				{
					path:    [][]byte{[]byte("$"), getVarInt(0)},
					segment: `[[]`,
				},
				{
					path:    [][]byte{[]byte("$")},
					segment: `]`,
				},
			},
		},
		{
			name: "nested nonempty array",
			json: `[[""]]`,
			segments: []indexedSegment{
				{
					path:    [][]byte{[]byte("$"), getVarInt(0), getVarInt(0)},
					segment: `[[""`,
				},
				{
					path:    [][]byte{[]byte("$"), getVarInt(0)},
					segment: `]`,
				},
				{
					path:    [][]byte{[]byte("$")},
					segment: `]`,
				},
			},
		},
		{
			name: "empty object",
			json: "{}",
			segments: []indexedSegment{
				{
					path:    [][]byte{[]byte("$")},
					segment: "{}",
				},
			},
		},
		{
			name: "object with one child",
			json: `{"a":1}`,
			segments: []indexedSegment{
				{
					path:    [][]byte{[]byte("$"), []byte("a")},
					segment: `{"a":1`,
				},
				{
					path:    [][]byte{[]byte("$")},
					segment: `}`,
				},
			},
		},
		{
			name: "object with many children",
			json: `{"a":1,"b":2,"c":3}`,
			segments: []indexedSegment{
				{
					path:    [][]byte{[]byte("$"), []byte("a")},
					segment: `{"a":1`,
				},
				{
					path:    [][]byte{[]byte("$"), []byte("b")},
					segment: `,"b":2`,
				},
				{
					path:    [][]byte{[]byte("$"), []byte("c")},
					segment: `,"c":3`,
				},
				{
					path:    [][]byte{[]byte("$")},
					segment: `}`,
				},
			},
		},
		{
			name: "nested empty object",
			json: `{"a":{}}`,
			segments: []indexedSegment{
				{
					path:    [][]byte{[]byte("$"), []byte("a")},
					segment: `{"a":{}`,
				},
				{
					path:    [][]byte{[]byte("$")},
					segment: `}`,
				},
			},
		},
		{
			name: "nested nonempty object",
			json: `{"a":{"b":5.0}}`,
			segments: []indexedSegment{
				{
					path:    [][]byte{[]byte("$"), []byte("a"), []byte("b")},
					segment: `{"a":{"b":5.0`,
				},
				{
					path:    [][]byte{[]byte("$"), []byte("a")},
					segment: `}`,
				},
				{
					path:    [][]byte{[]byte("$")},
					segment: `}`,
				},
			},
		},
		{
			name: "arrays and objects",
			json: `{"a":1,"b":[2,{"c":3,"d":[4,5],"e":6},7],"f":8}`,
			segments: []indexedSegment{
				{
					path:    [][]byte{[]byte("$"), []byte("a")},
					segment: `{"a":1`,
				},
				{
					path:    [][]byte{[]byte("$"), []byte("b"), getVarInt(0)},
					segment: `,"b":[2`,
				},
				{
					path:    [][]byte{[]byte("$"), []byte("b"), getVarInt(1), []byte("c")},
					segment: `,{"c":3`,
				},
				{
					path:    [][]byte{[]byte("$"), []byte("b"), getVarInt(1), []byte("d"), getVarInt(0)},
					segment: `,"d":[4`,
				},
				{
					path:    [][]byte{[]byte("$"), []byte("b"), getVarInt(1), []byte("d"), getVarInt(1)},
					segment: `,5`,
				},
				{
					path:    [][]byte{[]byte("$"), []byte("b"), getVarInt(1), []byte("d")},
					segment: `]`,
				},
				{
					path:    [][]byte{[]byte("$"), []byte("b"), getVarInt(1), []byte("e")},
					segment: `,"e":6`,
				},
				{
					path:    [][]byte{[]byte("$"), []byte("b"), getVarInt(1)},
					segment: `}`,
				},
				{
					path:    [][]byte{[]byte("$"), []byte("b"), getVarInt(2)},
					segment: `,7`,
				},
				{
					path:    [][]byte{[]byte("$"), []byte("b")},
					segment: `]`,
				},
				{
					path:    [][]byte{[]byte("$"), []byte("f")},
					segment: `,"f":8`,
				},
				{
					path:    [][]byte{[]byte("$")},
					segment: `}`,
				},
			},
		},
	}
	for _, test := range tests {
		t.Run("JSON splitting: "+test.name, func(t *testing.T) {
			decoder := ScanJsonFromBeginning([]byte(test.json))
			prevIndex := 0
			for _, pathAndSegment := range test.segments {
				decoder.AdvanceToNextLocation()
				actualSegment := string(decoder.jsonBuffer[prevIndex:decoder.valueOffset])
				prevIndex = decoder.valueOffset
				assert.Equal(t, pathAndSegment.segment, actualSegment)
				assert.Equal(t, pathAndSegment.path, decoder.currentPath)
			}
		})
	}
}
