package iceberg

import (
	"fmt"
	"reflect"
	"time"
)

// goReflectTypeToIceberg translates a Go type to its Iceberg SQL
// equivalent. v0 supports the common primitives; struct/slice types
// are serialized via the dorm `json` tag and end up as STRING.
func goReflectTypeToIceberg(t any) (string, error) {
	rt, ok := t.(reflect.Type)
	if !ok {
		return "", fmt.Errorf("iceberg: expected reflect.Type, got %T", t)
	}
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	switch rt.Kind() {
	case reflect.String:
		return "STRING", nil
	case reflect.Bool:
		return "BOOLEAN", nil
	case reflect.Int, reflect.Int64:
		return "BIGINT", nil
	case reflect.Int8, reflect.Int16, reflect.Int32:
		return "INT", nil
	case reflect.Float32:
		return "FLOAT", nil
	case reflect.Float64:
		return "DOUBLE", nil
	case reflect.Slice:
		if rt.Elem().Kind() == reflect.Uint8 {
			return "BINARY", nil
		}
		return "STRING", nil
	case reflect.Struct:
		if rt == reflect.TypeOf(time.Time{}) {
			return "TIMESTAMP", nil
		}
		return "STRING", nil
	default:
		return "STRING", nil
	}
}
