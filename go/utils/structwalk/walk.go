package structwalk

import (
	"reflect"
)

func Walk(v any, f func(sf reflect.StructField, depth int) error) error {
	return walkStruct(v, 0, f)
}

func walkStruct(v any, depth int, f func(sf reflect.StructField, depth int) error) error {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return walkFields(t, depth, f)
}

func walkFields(t reflect.Type, depth int, f func(sf reflect.StructField, depth int) error) error {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		err := processField(field, depth, f)
		if err != nil {
			return err
		}
	}

	return nil
}

func processField(field reflect.StructField, depth int, f func(sf reflect.StructField, depth int) error) error {
	if err := f(field, depth); err != nil {
		return err
	}

	if field.Type.Kind() == reflect.Ptr || field.Type.Kind() == reflect.Slice {
		if field.Type.Elem().Kind() == reflect.Struct {
			t := field.Type.Elem()
			if err := walkFields(t, depth+1, f); err != nil {
				return err
			}
		}
	} else if field.Type.Kind() == reflect.Struct {
		if err := walkFields(field.Type, depth+1, f); err != nil {
			return err
		}
	}

	return nil
}
