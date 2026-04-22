package structs

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/go-playground/validator/v10"
)

// validation is backed by go-playground/validator/v10 — the de
// facto Go validation library. Users declare rules with the
// standard `validate:"required,email,..."` struct tag and call
// lakeorm.Validate(records) at the application boundary.
//
// Why the switch from lake-orm's earlier custom registry: no value
// in reinventing a mature library that ships with 100+ built-in
// rules, cross-field comparisons, slice + map + struct traversal,
// locale-aware error messages, and a stable error type
// (validator.ValidationErrors) callers can type-assert for
// per-field handling. The old `validate=X` tag modifier on the
// lake tag is retired; rules now live on a standard
// `validate:"..."` tag alongside the lake tag.
//
// Example:
//
//	type User struct {
//	    ID    string `lake:"id,pk"              validate:"required,uuid"`
//	    Email string `lake:"email,mergeKey"     validate:"required,email"`
//	    Age   int    `lake:"age"                validate:"gte=18,lte=130"`
//	}
//
// Reads: lakeorm.Validate(&user) runs every registered rule; a
// failure returns an error that unwraps to validator.ValidationErrors:
//
//	if err := lakeorm.Validate(&user); err != nil {
//	    var vErrs validator.ValidationErrors
//	    if errors.As(err, &vErrs) {
//	        for _, fe := range vErrs {
//	            // fe.Field(), fe.Tag(), fe.Param(), fe.Value(), ...
//	        }
//	    }
//	}

var (
	validatorOnce sync.Once
	validatorInst *validator.Validate
)

// Validator returns the shared *validator.Validate instance. Call
// this once at init to register custom tags:
//
//	func init() {
//	    _ = lakeorm.Validator().RegisterValidation("uk_postcode", ukPostcode)
//	}
//
// Validate() uses the same instance, so registration is visible to
// every subsequent Validate call across the process.
func Validator() *validator.Validate {
	validatorOnce.Do(func() {
		validatorInst = validator.New(validator.WithRequiredStructEnabled())
	})
	return validatorInst
}

// Validate runs every registered rule against records — a pointer
// to a struct, a struct value, a slice of either, or nil.
//
// Call at the application boundary (HTTP handler, gRPC server, CLI
// entry point) before any Insert. A failure returns an error that
// unwraps to validator.ValidationErrors for per-field inspection.
//
// Shape-matching matches validator.Struct() semantics: pointers are
// dereferenced, slices are iterated element by element, nested
// structs traverse. See
// https://pkg.go.dev/github.com/go-playground/validator/v10 for the
// full tag grammar.
func Validate(records any) error {
	if records == nil {
		return nil
	}
	v := Validator()
	rv := reflect.ValueOf(records)
	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			return nil
		}
		return v.Struct(records)
	case reflect.Struct:
		return v.Struct(records)
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			item := rv.Index(i)
			for item.Kind() == reflect.Ptr {
				if item.IsNil() {
					item = reflect.Value{}
					break
				}
				item = item.Elem()
			}
			if !item.IsValid() {
				continue
			}
			if err := v.Struct(item.Interface()); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("lakeorm.Validate: unsupported type %v", rv.Kind())
	}
}
