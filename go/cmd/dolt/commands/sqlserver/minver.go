package sqlserver

import (
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/yaml.v2"

	"github.com/dolthub/dolt/go/libraries/utils/version"
)

func SerializeConfigForVersion(cfg YAMLConfig, versionNum uint32) ([]byte, error) {
	err := nullUnsupported(versionNum, &cfg)
	if err != nil {
		return nil, fmt.Errorf("error nulling unspported fields for version %d: %w", versionNum, err)
	}

	return yaml.Marshal(cfg)
}

func nullUnsupported(verNum uint32, st any) error {
	const tagName = "minver"

	// use reflection to loop over all fields in the struct st
	// for each field check the tag "minver" and if the current version is less than that, set the field to nil
	t := reflect.TypeOf(st)

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Iterate over all available fields and read the tag value
	for i := 0; i < t.NumField(); i++ {
		// Get the field, returns https://golang.org/pkg/reflect/#StructField
		field := t.Field(i)

		// Get the field tag value
		tag := field.Tag.Get(tagName)

		if tag != "" {
			// if it's nullable check to see if it should be set to nil
			if field.Type.Kind() == reflect.Ptr || field.Type.Kind() == reflect.Slice || field.Type.Kind() == reflect.Map {
				var setToNull bool

				if tag == "TBD" {
					setToNull = true
				} else {
					minver, err := version.Encode(tag)
					if err != nil {
						return fmt.Errorf("invalid version tag '%s' on field '%s': %w", tag, field.Name, err)
					}

					setToNull = verNum < minver
				}

				if setToNull {
					// Get the field value
					v := reflect.ValueOf(st).Elem().Field(i)
					v.Set(reflect.Zero(v.Type()))
				}
			} else {
				return fmt.Errorf("non-nullable field '%s' has a version tag '%s'", field.Name, tag)
			}

			var hasOmitEmpty bool
			yamlTag := field.Tag.Get("yaml")
			if yamlTag != "" {
				vals := strings.Split(yamlTag, ",")
				for _, val := range vals {
					if val == "omitempty" {
						hasOmitEmpty = true
						break
					}
				}
			}

			if !hasOmitEmpty {
				return fmt.Errorf("field '%s' has a version tag '%s' but no yaml tag with omitempty", field.Name, tag)
			}
		}

		v := reflect.ValueOf(st).Elem().Field(i)

		vIsNullable := v.Type().Kind() == reflect.Ptr || v.Type().Kind() == reflect.Slice || v.Type().Kind() == reflect.Map

		if !vIsNullable || !v.IsNil() {
			// if the field is a pointer to a struct, or a struct, or a slice recurse
			if field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct {
				err := nullUnsupported(verNum, v.Interface())
				if err != nil {
					return err
				}
			} else if field.Type.Kind() == reflect.Struct {
				err := nullUnsupported(verNum, v.Addr().Interface())
				if err != nil {
					return err
				}
			} else if field.Type.Kind() == reflect.Slice {
				if field.Type.Elem().Kind() == reflect.Ptr && field.Type.Elem().Elem().Kind() == reflect.Struct {
					for i := 0; i < v.Len(); i++ {
						err := nullUnsupported(verNum, v.Index(i).Interface())
						if err != nil {
							return err
						}
					}
				} else if field.Type.Elem().Kind() == reflect.Struct {
					for i := 0; i < v.Len(); i++ {
						err := nullUnsupported(verNum, v.Index(i).Addr().Interface())
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}

type MinVerFieldInfo struct {
	Name    string
	TypeStr string
	MinVer  string
	YamlTag string
}

func MinVerFieldInfoFromLine(l string) (MinVerFieldInfo, error) {
	tokens := strings.Split(l, " ")

	if len(tokens) != 4 {
		return MinVerFieldInfo{}, fmt.Errorf("invalid line in minver_validation.txt: '%s'", l)
	}

	return MinVerFieldInfo{
		Name:    tokens[0],
		TypeStr: tokens[1],
		MinVer:  tokens[2],
		YamlTag: tokens[3],
	}, nil
}

func MinVerFieldInfoFromStructField(field reflect.StructField, depth int) MinVerFieldInfo {
	info := MinVerFieldInfo{
		Name:    strings.Repeat("-", depth) + field.Name,
		TypeStr: field.Type.String(),
		MinVer:  field.Tag.Get("minver"),
		YamlTag: field.Tag.Get("yaml"),
	}

	if info.MinVer == "" {
		info.MinVer = "0.0.0"
	}

	return info
}

func (fi MinVerFieldInfo) Equals(other MinVerFieldInfo) bool {
	return fi.Name == other.Name && fi.TypeStr == other.TypeStr && fi.MinVer == other.MinVer && fi.YamlTag == other.YamlTag
}

func (fi MinVerFieldInfo) String() string {
	return fmt.Sprintf("%s %s %s %s", fi.Name, fi.TypeStr, fi.MinVer, fi.YamlTag)
}
