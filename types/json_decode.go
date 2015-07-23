package types

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

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
		return FutureFromValue(Bool(v)), nil
	case string:
		return FutureFromValue(NewString(v)), nil
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
		case "int16", "int32", "int64", "uint16", "uint32", "uint64", "float32", "float64":
			// Go decodes all JSON numbers as float64
			if v, ok := v.(float64); ok {
				switch k {
				case "int16":
					return FutureFromValue(Int16(int16(v))), nil
				case "int32":
					return FutureFromValue(Int32(int32(v))), nil
				case "int64":
					return FutureFromValue(Int64(int64(v))), nil
				case "uint16":
					return FutureFromValue(UInt16(uint16(v))), nil
				case "uint32":
					return FutureFromValue(UInt32(uint32(v))), nil
				case "uint64":
					return FutureFromValue(UInt64(uint64(v))), nil
				case "float32":
					return FutureFromValue(Float32(float32(v))), nil
				case "float64":
					return FutureFromValue(Float64(float64(v))), nil
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
	return FutureFromValue(listFromFutures(output, s)), nil
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
	return FutureFromValue(setFromFutures(output, s)), nil
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

	return FutureFromValue(mapFromFutures(output, s)), nil
}

func jsonDecodeRef(refStr string, s chunks.ChunkSource) (Future, error) {
	ref, err := ref.Parse(refStr)
	if err != nil {
		return nil, err
	}
	return FutureFromRef(ref), nil
}
