package enc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

var errInvalidEncoding = errors.New("Invalid encoding")

func jsonDecode(reader io.Reader) interface{} {
	prefix := make([]byte, len(jsonTag))
	_, err := io.ReadFull(reader, prefix)
	d.Exp.NoError(err)

	// Since jsonDecode is private, and Decode() should have checked this, it is invariant that the prefix will match.
	d.Chk.EqualValues(jsonTag[:], prefix, "Cannot jsonDecode - invalid prefix")

	var v interface{}
	err = json.NewDecoder(reader).Decode(&v)
	d.Exp.NoError(err)

	return jsonDecodeValue(v)
}

func jsonDecodeValue(v interface{}) interface{} {
	switch v := v.(type) {
	case bool, string:
		return v
	case map[string]interface{}:
		return jsonDecodeTaggedValue(v)
	default:
		d.Exp.Fail(fmt.Sprintf("Cannot decode json value: %+v", v))
	}
	return nil
}

func jsonDecodeTaggedValue(m map[string]interface{}) interface{} {
	d.Chk.Len(m, 1)
	for k, v := range m {
		switch k {
		case "cb":
			if v, ok := v.([]interface{}); ok {
				return jsonDecodeCompoundBlob(v)
			}
		case "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float32", "float64":
			// Go decodes all JSON numbers as float64
			if v, ok := v.(float64); ok {
				switch k {
				case "int8":
					return int8(v)
				case "int16":
					return int16(v)
				case "int32":
					return int32(v)
				case "int64":
					return int64(v)
				case "uint8":
					return uint8(v)
				case "uint16":
					return uint16(v)
				case "uint32":
					return uint32(v)
				case "uint64":
					return uint64(v)
				case "float32":
					return float32(v)
				case "float64":
					return float64(v)
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
	d.Exp.Fail(fmt.Sprintf("Cannot decode tagged json value: %+v", m))
	return nil
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
func jsonDecodeCompoundBlob(input []interface{}) interface{} {
	if len(input)%2 != 0 || len(input) < 2 {
		d.Exp.NoError(errInvalidEncoding)
	}

	offset := uint64(0)
	numBlobs := len(input) / 2
	offsets := make([]uint64, numBlobs)
	blobs := make([]ref.Ref, numBlobs)

	ensureRef := func(v interface{}) ref.Ref {
		// Consider rejiggering this error handling with BUG #176.
		if v, ok := v.(ref.Ref); !ok {
			d.Exp.Fail(fmt.Sprintf("CompoundBlob children must be ref.Refs; got %+v", v))
			return ref.Ref{}
		}
		return v.(ref.Ref)
	}

	for i := 0; i < len(input); i += 2 {
		blobs[i/2] = ensureRef(jsonDecodeValue(input[i]))
		length, err := toUint64(input[i+1])
		d.Exp.NoError(err)
		offset += length
		offsets[i/2] = offset
	}

	return CompoundBlob{offsets, blobs}
}

func jsonDecodeList(input []interface{}) []interface{} {
	output := make([]interface{}, len(input))
	for i, inVal := range input {
		output[i] = jsonDecodeValue(inVal)
	}
	return output
}

func jsonDecodeMap(input []interface{}) Map {
	r := jsonDecodeList(input)
	return MapFromItems(r...)
}

func jsonDecodeSet(input []interface{}) Set {
	return jsonDecodeList(input)
}
