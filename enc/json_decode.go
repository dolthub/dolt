package enc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

var errInvalidEncoding = errors.New("Invalid encoding")

func jsonDecode(reader io.Reader) (interface{}, error) {
	prefix := make([]byte, len(jsonTag))
	_, err := io.ReadFull(reader, prefix)
	if err != nil {
		return nil, err
	}

	// Since jsonDecode is private, and Decode() should have checked this, it is invariant that the prefix will match.
	dbg.Chk.EqualValues(jsonTag[:], prefix, "Cannot jsonDecode - invalid prefix")

	var v interface{}
	err = json.NewDecoder(reader).Decode(&v)
	if err != nil {
		return nil, err
	}

	r, err := jsonDecodeValue(v)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func jsonDecodeValue(v interface{}) (interface{}, error) {
	switch v := v.(type) {
	case bool, string:
		return v, nil
	case map[string]interface{}:
		return jsonDecodeTaggedValue(v)
	default:
		return nil, fmt.Errorf("Cannot decode json value: %+v", v)
	}
}

func jsonDecodeTaggedValue(m map[string]interface{}) (interface{}, error) {
	dbg.Chk.Len(m, 1)
	for k, v := range m {
		switch k {
		case "cb":
			if v, ok := v.([]interface{}); ok {
				return jsonDecodeCompoundBlob(v)
			}
		case "int16", "int32", "int64", "uint16", "uint32", "uint64", "float32", "float64":
			// Go decodes all JSON numbers as float64
			if v, ok := v.(float64); ok {
				switch k {
				case "int16":
					return int16(v), nil
				case "int32":
					return int32(v), nil
				case "int64":
					return int64(v), nil
				case "uint16":
					return uint16(v), nil
				case "uint32":
					return uint32(v), nil
				case "uint64":
					return uint64(v), nil
				case "float32":
					return float32(v), nil
				case "float64":
					return float64(v), nil
				}
			}
		case "list":
			if v, ok := v.([]interface{}); ok {
				return jsonDecodeList(v)
			}
		case "map":
			if v, ok := v.([]interface{}); ok {
				return jsonDecodeMap(v)
			}
		case "ref":
			if v, ok := v.(string); ok {
				return ref.Parse(v)
			}
		case "set":
			if v, ok := v.([]interface{}); ok {
				return jsonDecodeSet(v)
			}
		}
		break
	}
	return nil, fmt.Errorf("Cannot decode tagged json value: %+v", m)
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

// [{"ref":"sha1-0"}, length0, ... {"ref":"sha1-N"},lengthN]
func jsonDecodeCompoundBlob(input []interface{}) (interface{}, error) {
	if len(input)%2 != 0 || len(input) < 2 {
		return nil, errInvalidEncoding
	}

	offset := uint64(0)
	numBlobs := len(input) / 2
	offsets := make([]uint64, numBlobs)
	blobs := make([]ref.Ref, numBlobs)

	ensureRef := func(v interface{}, err error) (ref.Ref, error) {
		if err != nil {
			return ref.Ref{}, err
		}
		// Consider rejiggering this error handling with BUG #176.
		if v, ok := v.(ref.Ref); !ok {
			return ref.Ref{}, fmt.Errorf("CompoundBlob children must be ref.Refs; got %+v", v)
		}
		return v.(ref.Ref), nil
	}

	for i := 0; i < len(input); i += 2 {
		var err error
		blobs[i/2], err = ensureRef(jsonDecodeValue(input[i]))
		if err != nil {
			return nil, err
		}
		length, err := toUint64(input[i+1])
		if err != nil {
			return nil, err
		}
		offset += length
		offsets[i/2] = offset
	}

	return CompoundBlob{offsets, blobs}, nil
}

func jsonDecodeList(input []interface{}) ([]interface{}, error) {
	output := make([]interface{}, len(input))
	for i, inVal := range input {
		outVal, err := jsonDecodeValue(inVal)
		if err != nil {
			return nil, err
		}
		output[i] = outVal
	}
	return output, nil
}

func jsonDecodeMap(input []interface{}) (Map, error) {
	r, err := jsonDecodeList(input)
	if err != nil {
		return nil, err
	}
	return MapFromItems(r...), nil
}

func jsonDecodeSet(input []interface{}) (Set, error) {
	return jsonDecodeList(input)
}
