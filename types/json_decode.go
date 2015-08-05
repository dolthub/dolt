package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

var errInvalidEncoding = errors.New("Invalid encoding")

func jsonDecode(reader io.Reader, s chunks.ChunkSource) (Value, error) {
	prefix := make([]byte, len(jsonTag))
	_, err := io.ReadFull(reader, prefix)
	if err != nil {
		return nil, err
	}

	// Since jsonDecode is private, and ReadValue() should have checked this, it is invariant that the prefix will match.
	Chk.EqualValues(jsonTag[:], prefix, "Cannot jsonDecode - invalid prefix")

	var v interface{}
	err = json.NewDecoder(reader).Decode(&v)
	if err != nil {
		return nil, err
	}

	f, err := jsonDecodeValue(v, s)
	if err != nil {
		return nil, err
	}

	val, err := f.Deref(s)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func jsonDecodeValue(v interface{}, s chunks.ChunkSource) (Future, error) {
	switch v := v.(type) {
	case bool:
		return futureFromValue(Bool(v)), nil
	case string:
		return futureFromValue(NewString(v)), nil
	case map[string]interface{}:
		return jsonDecodeTaggedValue(v, s)
	default:
		return nil, fmt.Errorf("Cannot decode json value: %+v", v)
	}
}

func jsonDecodeTaggedValue(m map[string]interface{}, s chunks.ChunkSource) (Future, error) {
	Chk.Equal(1, len(m))
	for k, v := range m {
		switch k {
		case "cb":
			if v, ok := v.([]interface{}); ok {
				return jsonDecodeCompoundBlob(v, s)
			}
		case "int16", "int32", "int64", "uint16", "uint32", "uint64", "float32", "float64":
			// Go decodes all JSON numbers as float64
			if v, ok := v.(float64); ok {
				switch k {
				case "int16":
					return futureFromValue(Int16(int16(v))), nil
				case "int32":
					return futureFromValue(Int32(int32(v))), nil
				case "int64":
					return futureFromValue(Int64(int64(v))), nil
				case "uint16":
					return futureFromValue(UInt16(uint16(v))), nil
				case "uint32":
					return futureFromValue(UInt32(uint32(v))), nil
				case "uint64":
					return futureFromValue(UInt64(uint64(v))), nil
				case "float32":
					return futureFromValue(Float32(float32(v))), nil
				case "float64":
					return futureFromValue(Float64(float64(v))), nil
				}
			}
		case "list":
			if v, ok := v.([]interface{}); ok {
				return jsonDecodeList(v, s)
			}
		case "set":
			if v, ok := v.([]interface{}); ok {
				return jsonDecodeSet(v, s)
			}
		case "map":
			if v, ok := v.([]interface{}); ok {
				return jsonDecodeMap(v, s)
			}
		case "ref":
			if v, ok := v.(string); ok {
				return jsonDecodeRef(v, s)
			}
		}
		break
	}
	return nil, fmt.Errorf("Cannot decode tagged json value: %+v", m)
}

func jsonDecodeList(input []interface{}, s chunks.ChunkSource) (Future, error) {
	output := []Future{}
	for _, inVal := range input {
		outVal, err := jsonDecodeValue(inVal, s)
		if err != nil {
			return nil, err
		}
		output = append(output, outVal)
	}
	return futureFromValue(listFromFutures(output, s)), nil
}

func jsonDecodeSet(input []interface{}, s chunks.ChunkSource) (Future, error) {
	output := []Future{}
	for _, inVal := range input {
		f, err := jsonDecodeValue(inVal, s)
		if err != nil {
			return nil, err
		}
		output = append(output, f)
	}
	return futureFromValue(setFromFutures(output, s)), nil
}

func jsonDecodeMap(input []interface{}, s chunks.ChunkSource) (Future, error) {
	output := []Future{}
	Chk.Equal(0, len(input)%2, "Length on input array must be multiple of 2")

	for _, inVal := range input {
		f, err := jsonDecodeValue(inVal, s)
		if err != nil {
			return nil, err
		}
		output = append(output, f)
	}

	return futureFromValue(mapFromFutures(output, s)), nil
}

func jsonDecodeRef(refStr string, s chunks.ChunkSource) (Future, error) {
	ref, err := ref.Parse(refStr)
	if err != nil {
		return nil, err
	}
	return futureFromRef(ref), nil
}

func toUint64(v interface{}) (uint64, error) {
	fl, ok := v.(float64)
	if !ok {
		return 0, errInvalidEncoding
	}
	i := uint64(fl)
	if float64(i) != fl {
		return 0, errInvalidEncoding
	}
	return i, nil
}

// [{"ref":"sha1-0"},offset, ... offset, {"ref":"sha1-N"},length]
func jsonDecodeCompoundBlob(input []interface{}, cs chunks.ChunkSource) (Future, error) {
	if len(input)%2 != 0 || len(input) < 2 {
		return nil, errInvalidEncoding
	}

	length, err := toUint64(input[len(input)-1])
	if err != nil {
		return nil, err
	}

	numBlobs := len(input) / 2
	offsets := make([]uint64, numBlobs)
	blobs := make([]Future, numBlobs)

	for i := 0; i < len(input)-1; i++ {
		var err error
		var offset uint64
		if i == 0 {
			offset = uint64(0)
		} else {
			offset, err = toUint64(input[i])
			i++
			if err != nil {
				return nil, err
			}
		}
		offsets[i/2] = offset
		blobs[i/2], err = jsonDecodeValue(input[i], cs)
		if err != nil {
			return nil, err
		}
	}

	cb := compoundBlob{length, offsets, blobs, &ref.Ref{}, cs}
	return futureFromValue(cb), nil
}
