package config

// MapConfig is a simple config for in memory or test configuration.  Calls to SetStrings will are valid for the
// lifecycle of a program, but are not persisted anywhere and will return to their default values on the next run
// of a program.
type MapConfig struct {
	properties map[string]string
}

// NewMapConfig creates a config from a map.
func NewMapConfig(properties map[string]string) *MapConfig {
	return &MapConfig{properties}
}

// GetString retrieves a value for a given key.
func (mc *MapConfig) GetString(k string) (string, error) {
	if val, ok := mc.properties[k]; ok {
		return val, nil
	}

	return "", ErrConfigParamNotFound
}

// SetString sets the values for a map of updates.
func (mc *MapConfig) SetStrings(updates map[string]string) error {
	for k, v := range updates {
		mc.properties[k] = v
	}

	return nil
}

// Iter will perform a callback for ech value in a config until all values have been exhausted or until the
// callback returns true indicating that it should stop.
func (mc *MapConfig) Iter(cb func(string, string) (stop bool)) {
	for k, v := range mc.properties {
		stop := cb(k, v)

		if stop {
			break
		}
	}
}

// Unset removes a configuration parameter from the config
func (mc *MapConfig) Unset(params []string) error {
	for _, param := range params {
		delete(mc.properties, param)
	}

	return nil
}

// Size returns the number of properties contained within the config
func (mc *MapConfig) Size() int {
	return len(mc.properties)
}
