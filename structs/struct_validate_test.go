package structs

import (
	"errors"
	"testing"

	"github.com/go-playground/validator/v10"
)

// valUser declares validation rules via the standard go-playground
// `validate:"..."` tag. lake-orm's own tag is unrelated and doesn't
// carry validation modifiers anymore.
type valUser struct {
	Email   string `lake:"email,mergeKey" validate:"required,email"`
	Country string `lake:"country"        validate:"required"`
}

func TestValidate_RequiredEmpty(t *testing.T) {
	u := &valUser{}
	err := Validate(u)
	var ve validator.ValidationErrors
	if !errors.As(err, &ve) {
		t.Fatalf("expected validator.ValidationErrors, got %T: %v", err, err)
	}
	if !hasFieldFailure(ve, "Email") {
		t.Errorf("expected Email failure, got %v", ve)
	}
	if !hasFieldFailure(ve, "Country") {
		t.Errorf("expected Country failure, got %v", ve)
	}
}

func TestValidate_EmailInvalid(t *testing.T) {
	u := &valUser{Email: "not-an-email", Country: "UK"}
	err := Validate(u)
	var ve validator.ValidationErrors
	if !errors.As(err, &ve) {
		t.Fatalf("expected validator.ValidationErrors, got %v", err)
	}
	if !hasFieldFailure(ve, "Email") {
		t.Errorf("expected Email failure for invalid address")
	}
}

func TestValidate_Pass(t *testing.T) {
	u := &valUser{Email: "alice@example.com", Country: "UK"}
	if err := Validate(u); err != nil {
		t.Errorf("expected validation to pass, got: %v", err)
	}
}

func TestValidate_SliceStopsOnFirstInvalid(t *testing.T) {
	// go-playground/validator returns an error per Struct() call;
	// iterating a slice returns the first failure rather than an
	// aggregated report. Intentional — matches the library's
	// semantics and keeps the implementation cheap.
	users := []*valUser{
		{Email: "alice@example.com", Country: "UK"},
		{Email: "bad"},
	}
	err := Validate(users)
	if err == nil {
		t.Fatal("expected validation to fail on the bad record")
	}
	var ve validator.ValidationErrors
	if !errors.As(err, &ve) {
		t.Fatalf("expected validator.ValidationErrors, got %T: %v", err, err)
	}
}

func TestValidate_CustomValidationRegistration(t *testing.T) {
	type rec struct {
		V string `lake:"v" validate:"upperonly"`
	}
	err := Validator().RegisterValidation("upperonly", func(fl validator.FieldLevel) bool {
		s := fl.Field().String()
		for _, r := range s {
			if r < 'A' || r > 'Z' {
				return false
			}
		}
		return true
	})
	if err != nil {
		t.Fatalf("RegisterValidation: %v", err)
	}

	if err := Validate(&rec{V: "HELLO"}); err != nil {
		t.Errorf("upperonly should pass on HELLO: %v", err)
	}
	if err := Validate(&rec{V: "Hello"}); err == nil {
		t.Error("upperonly should fail on Hello")
	}
}

// hasFieldFailure is a tiny helper to assert a field-keyed failure
// showed up in a ValidationErrors without walking the slice inline
// in every test.
func hasFieldFailure(errs validator.ValidationErrors, field string) bool {
	for _, fe := range errs {
		if fe.Field() == field {
			return true
		}
	}
	return false
}
