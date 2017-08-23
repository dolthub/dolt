package common

import (
	"fmt"
	"strings"
)

func MapGetKV(v map[string]interface{}, key string) (interface{}, error) {
	var ok bool
	var mcursor map[string]interface{}
	var cursor interface{} = v

	parts := strings.Split(key, ".")
	for i, part := range parts {
		sofar := strings.Join(parts[:i], ".")

		mcursor, ok = cursor.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("%s key is not a map", sofar)
		}

		cursor, ok = mcursor[part]
		if !ok {
			return nil, fmt.Errorf("%s key has no attributes", sofar)
		}
	}
	return cursor, nil
}

func MapSetKV(v map[string]interface{}, key string, value interface{}) error {
	var ok bool
	var mcursor map[string]interface{}
	var cursor interface{} = v

	parts := strings.Split(key, ".")
	for i, part := range parts {
		mcursor, ok = cursor.(map[string]interface{})
		if !ok {
			sofar := strings.Join(parts[:i], ".")
			return fmt.Errorf("%s key is not a map", sofar)
		}

		// last part? set here
		if i == (len(parts) - 1) {
			mcursor[part] = value
			break
		}

		cursor, ok = mcursor[part]
		if !ok || cursor == nil { // create map if this is empty or is null
			mcursor[part] = map[string]interface{}{}
			cursor = mcursor[part]
		}
	}
	return nil
}
