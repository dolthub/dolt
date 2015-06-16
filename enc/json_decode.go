package enc

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

func jsonDecode(reader io.Reader, s chunks.ChunkSource) (types.Value, error) {
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

	return jsonDecodeValue(v, s)
}

func jsonDecodeValue(v interface{}, s chunks.ChunkSource) (types.Value, error) {
	switch v := v.(type) {
	case bool:
		return types.Bool(v), nil
	case string:
		return types.NewString(v), nil
	case map[string]interface{}:
		return jsonDecodeTaggedValue(v, s)
	default:
		return nil, fmt.Errorf("Cannot decode json value: %+v", v)
	}
}

func jsonDecodeTaggedValue(m map[string]interface{}, s chunks.ChunkSource) (types.Value, error) {
	Chk.Equal(1, len(m))
	for k, v := range m {
		switch k {
		case "int16", "int32", "int64", "uint16", "uint32", "uint64", "float32", "float64":
			// Go decodes all JSON numbers as float64
			if v, ok := v.(float64); ok {
				switch k {
				case "int16":
					return types.Int16(int16(v)), nil
				case "int32":
					return types.Int32(int32(v)), nil
				case "int64":
					return types.Int64(int64(v)), nil
				case "uint16":
					return types.UInt16(uint16(v)), nil
				case "uint32":
					return types.UInt32(uint32(v)), nil
				case "uint64":
					return types.UInt64(uint64(v)), nil
				case "float32":
					return types.Float32(float32(v)), nil
				case "float64":
					return types.Float64(float64(v)), nil
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

func jsonDecodeList(input []interface{}, s chunks.ChunkSource) (types.Value, error) {
	output := types.NewList()
	for _, inVal := range input {
		outVal, err := jsonDecodeValue(inVal, s)
		if err != nil {
			return nil, err
		}
		output = output.Append(outVal)
	}
	return output, nil
}

func jsonDecodeSet(input []interface{}, s chunks.ChunkSource) (types.Value, error) {
	vals := []types.Value{}
	for _, inVal := range input {
		outVal, err := jsonDecodeValue(inVal, s)
		if err != nil {
			return nil, err
		}
		vals = append(vals, outVal)
	}
	return types.NewSet(vals...), nil
}

func jsonDecodeMap(input []interface{}, s chunks.ChunkSource) (types.Value, error) {
	output := types.NewMap()
	Chk.Equal(0, len(input)%2, "Length on input array must be multiple of 2")
	for i := 0; i < len(input); i += 2 {
		inKey := input[i]
		inVal := input[i+1]

		outKey, err := jsonDecodeValue(inKey, s)
		if err != nil {
			return nil, err
		}
		outVal, err := jsonDecodeValue(inVal, s)
		if err != nil {
			return nil, err
		}
		output = output.Set(outKey, outVal)
	}
	return output, nil
}

func jsonDecodeRef(refStr string, s chunks.ChunkSource) (types.Value, error) {
	ref, err := ref.Parse(refStr)
	if err != nil {
		return nil, err
	}
	return ReadValue(ref, s)
}
