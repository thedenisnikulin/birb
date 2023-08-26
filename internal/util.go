package internal

import (
	"reflect"
)

func FieldValueByTag(v any, tag, tagValue string) (reflect.Value, bool) {
	stype := reflect.TypeOf(v)
	sval := reflect.ValueOf(v)
	for i := 0; i < stype.NumField(); i++ {
		f := stype.Field(i)
		val, ok := f.Tag.Lookup(tag)
		if ok && val == tagValue {
			return sval.FieldByName(f.Name), true
		}
	}

	return reflect.Value{}, false
}
